import { MongoClient, Collection, Db as MongoDb } from 'mongodb';
import { Db as HirelingDb, DbEvent } from 'hireling/db';
import { JobId, JobAttr, JobStatus } from 'hireling/job';
import { WorkerId } from 'hireling/worker';

interface MongoAttr {
  _id: JobId;
}

// basic types to help refactoring
type MongoIncludeFields<T> = {
  [P in keyof T]?: 1;
};

// interface MongoPredicate<T, P extends keyof T> {
//   $lt?: T[P];
// }

// type MongoQueryFilter<T> = {
//   [P in keyof T]?: T[P]|MongoPredicate<T, P>;
// };

export const MONGO_DEFS = {
  uri:        'mongodb://localhost:27017',
  database:   'hireling',
  collection: 'jobs',
  opts:       ({} as any) as object // pass-through client options, merged in
};

export type MongoOpt = typeof MONGO_DEFS;

export class MongoEngine extends HirelingDb {
  private db: MongoDb;
  private coll: Collection<MongoAttr>;
  private readonly dbc: MongoOpt;

  constructor(opt?: Partial<MongoOpt>) {
    super();

    this.log.debug('db created (mongo)');

    this.dbc = { ...MONGO_DEFS, ...opt };
  }

  open() {
    MongoClient.connect(`${this.dbc.uri}/${this.dbc.database}`, {
      autoReconnect: false, // handle manually, no query replaying
      poolSize:      10,
      ...this.dbc.opts
    }, async (err, db) => {
      if (err) {
        this.log.error('could not connect to db', err);
        this.event(DbEvent.close, err);
        return;
      }

      this.log.debug('opened');

      db.on('close', async (err: Error) => {
        this.log.warn('closed', err);

        if (err) {
          this.log.info('force closing');

          await this.db.close(true);
        }
        else {
          this.event(DbEvent.close);
        }
      });

      // db.on('reconnect', () => {
      //   this.log.debug('opened (reconnect)');
      //   this.event(DbEvent.open);
      // });

      this.db = db;
      this.coll = this.db.collection(this.dbc.collection);

      const indexes: MongoIncludeFields<JobAttr>[] = [
        { status: 1 }
      ];

      for (const i of indexes) {
        await this.coll.createIndex(i);
      }

      this.event(DbEvent.open);
    });
  }

  close(force = false) {
    this.log.warn(`closing - ${force ? 'forced' : 'graceful'}`);

    this.db.close(force, (err) => {
      if (err) {
        this.log.error('close error', err);
      }
    });
  }

  async add(job: JobAttr) {
    this.log.debug(`add job ${job.id}`);

    const { insertedCount } = await this.coll
      .insertOne(MongoEngine.toMongo(job));

    if (insertedCount !== 1) {
      throw new Error('could not create job');
    }
  }

  async getById(id: JobId) {
    this.log.debug('get job by id');

    const mjob = await this.coll.findOne({ _id: id });

    return mjob ? MongoEngine.fromMongo<JobAttr>(mjob) : null;
  }

  async get(query: Partial<JobAttr>) {
    this.log.debug('search jobs');

    const mjob = await this.coll.find(MongoEngine.toMongo(query)).toArray();

    return mjob.map(m => MongoEngine.fromMongo<JobAttr>(m));
  }

  async reserve(wId: WorkerId) {
    this.log.debug(`atomic reserve job ${wId}`);

    const from: JobStatus = 'ready';
    const to: JobStatus  = 'processing';

    const filter: Partial<JobAttr> = { status: from };
    const update: Partial<JobAttr> = { status: to, workerid: wId };

    const { value } = await this.coll.findOneAndUpdate(
      filter,
      { $set: update },
      { returnOriginal: false }
    );

    return value ? MongoEngine.fromMongo<JobAttr>(value) : null;
  }

  async updateById(id: JobId, values: Partial<JobAttr>) {
    this.log.debug('update job');

    const { modifiedCount } = await this.coll.updateOne(
      { _id: id },
      { $set: values }
    );

    if (modifiedCount !== 1) {
      throw new Error(`updated ${modifiedCount || 0} jobs instead of 1`);
    }
  }

  async removeById(id: JobId) {
    this.log.debug('remove job by id');

    const { deletedCount } = await this.coll.deleteOne({ _id: id });

    if (deletedCount !== 1) {
      throw new Error(`deleted ${deletedCount || 0} jobs instead of 1`);
    }

    return true;
  }

  async remove(query: Partial<JobAttr>) {
    this.log.debug('remove jobs');

    const filter = MongoEngine.toMongo(query);

    const { deletedCount } = await this.coll.deleteMany(filter);

    return deletedCount || 0;
  }

  async clear() {
    const { deletedCount } = await this.coll.deleteMany({});

    return deletedCount || 0;
  }

  private static fromMongo<T>(mobj: any) {
    if (mobj._id) {
      const obj = { id: mobj._id, ...mobj };
      delete obj._id;

      return obj as T;
    }

    return mobj as T;
  }

  private static toMongo(obj: any) {
    if (obj.id) {
      const mobj = { _id: obj.id, ...obj };
      delete mobj.id;

      return mobj;
    }

    return obj;
  }
}
