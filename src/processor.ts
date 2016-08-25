import { Processor, Config, ModuleFunction, DoneFunction } from 'hive-processor';
import { Client as PGClient, ResultSet } from 'pg';
import { createClient, RedisClient} from 'redis';
import * as bunyan from 'bunyan';

let log = bunyan.createLogger({
  name: 'plan-processor',
  streams: [
    {
      level: 'info',
      path: '/var/log/processor-info.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1d',   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: 'error',
      path: '/var/log/processor-error.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1w',   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

let config: Config = {
  dbhost: process.env['DB_HOST'],
  dbuser: process.env['DB_USER'],
  database: process.env['DB_NAME'],
  dbpasswd: process.env['DB_PASSWORD'],
  cachehost: process.env['CACHE_HOST'],
  addr: "ipc:///tmp/queue.ipc"
};

let processor = new Processor(config);

processor.call('refresh', (db: PGClient, cache: RedisClient, done: DoneFunction) => {
  log.info('refresh');
  db.query('SELECT id, title, description, image, thumbnail, period FROM plans', [], (err: Error, result: ResultSet) => {
    if (err) {
      log.error(err, 'query error');
      return;
    }
    let plans = [];
    for (let row of result.rows) {
      plans.push(row2plan(row));
    }
    let countdown = plans.length; // indicate how many async callings are running
    for (let plan of plans) {
      db.query('SELECT id, name, title, description FROM plan_rules WHERE pid = $1', [ plan.id ], (err1: Error, result1: ResultSet) => {
        countdown -= 1;
        if (err1) {
          log.error(err1, 'query error');
        } else {
          for (let row of result1.rows) {
            plan.rules.push(row2rule(row));
          }
        }
        if (countdown == 0) {
          // all query are done
          let multi = cache.multi();
          for (let plan of plans) {
            multi.hset("plan-entities", plan.id, JSON.stringify(plan));
          }
          for (let plan of plans) {
            multi.sadd("plans", plan.id);
          }
          multi.exec((err2, replies) => {
            if (err2) {
              log.error(err2);
            }
            done(); // close db and cache connection
          });
        }
      });
    }
  });
});

function row2plan(row) {
  return {
    id: row.id,
    title: row.title? row.title.trim(): '',
    description: row.description,
    image: row.image? row.image.trim(): '',
    thumbnail: row.thumbnail? row.thumbnail.trim(): '',
    period: row.period,
    rules: []
  };
}

function row2rule(row) {
  return {
    id: row.id,
    name: row.name? row.name.trim(): '',
    title: row.title? row.name.trim(): '',
    description: row.description
  };
}

log.info('Start processor at %s', config.addr);

processor.run();
