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

interface MongoPredicate<T, P extends keyof T> {
  $lt?: T[P];
}

type MongoQueryFilter<T> = {
  [P in keyof T]?: T[P]|MongoPredicate<T, P>;
};

export const MONGO_DEFS = {
  uri:         'mongodb://localhost:27017/hireling',
  collection: 'jobs'
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
    MongoClient.connect(this.dbc.uri, {
      autoReconnect: false // handle manually, no query replaying
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

      const i1: MongoIncludeFields<JobAttr> = { status: 1, expires: 1 };
      const i2: MongoIncludeFields<JobAttr> = { status: 1, stalls: 1 };

      await this.coll.createIndex(i1);
      await this.coll.createIndex(i2);

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

  async clear() {
    const { deletedCount } = await this.coll.deleteMany({});

    return deletedCount || 0;
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

  async atomicFindReady(wId: WorkerId) {
    this.log.debug(`atomic find update job ${wId}`);

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

  async removeByStatus(status: JobStatus) {
    this.log.debug(`remove jobs by status [${status}]`);

    const filter: Partial<JobAttr> = { status };

    const { deletedCount } = await this.coll.deleteMany(filter);

    return deletedCount || 0;
  }

  async refreshExpired() {
    this.log.debug('refresh expired jobs');

    const now = Date.now();
    let updated = 0;

    const query: MongoQueryFilter<JobAttr> = {
      status:  'processing',
      expires: { $lt: new Date() }
    };

    type ExpiredFields = Pick<JobAttr, 'id'|'attempts'|'expires'|'expirems'>;

    const inc: MongoIncludeFields<ExpiredFields> = {
      attempts: 1, expires: 1, expirems: 1
    };

    const jobs = await this.coll
      .find(query).project(inc).snapshot(true).toArray();

    for (const jm of jobs) {
      const j = MongoEngine.fromMongo<ExpiredFields>(jm);

      const update: Partial<JobAttr> = {
        status:   'ready',
        expires:  new Date(now + j.expirems!),
        attempts: j.attempts + 1
      };

      const { modifiedCount } = await this.coll.updateOne(
        { _id: j.id },
        { $set: update }
      );

      updated += modifiedCount;
    }

    return updated;
  }

  async refreshStalled() {
    this.log.debug('refresh stalled jobs');

    const now = Date.now();
    let updated = 0;

    const query: MongoQueryFilter<JobAttr> = {
      status: 'processing',
      stalls: { $lt: new Date() }
    };

    type StalledFields = Pick<JobAttr, 'id'|'attempts'|'stalls'|'stallms'>;

    const inc: MongoIncludeFields<StalledFields> = {
      attempts: 1, stalls: 1, stallms: 1
    };

    const jobs = await this.coll
      .find(query).project(inc).snapshot(true).toArray();

    for (const jm of jobs) {
      const j = MongoEngine.fromMongo<StalledFields>(jm);

      const update: Partial<JobAttr> = {
        status:   'ready',
        stalls:   new Date(now + j.stallms!),
        attempts: j.attempts + 1
      };

      const { modifiedCount } = await this.coll.updateOne(
        { _id: j.id },
        { $set: update }
      );

      updated += modifiedCount;
    }

    return updated;
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
