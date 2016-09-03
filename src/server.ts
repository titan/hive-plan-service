import { Server, Config, Context, ResponseFunction, Permission } from 'hive-server';
import * as Redis from "redis";
import * as nanomsg from 'nanomsg';
import * as msgpack from 'msgpack-lite';
import * as bunyan from 'bunyan';

let log = bunyan.createLogger({
  name: 'plan-server',
  streams: [
    {
      level: 'info',
      path: '/var/log/server-info.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1d',   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: 'error',
      path: '/var/log/server-error.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1w',   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

let redis = Redis.createClient(6379, process.env['CACHE_HOST']); // port, host

let list_key = "plans";
let entities_prefix = "plans-";
let items_prefix = "plan-items-";
let entity_key = "plan-entities";
let item_key = "plan-items";

let config: Config = {
  svraddr: 'tcp://0.0.0.0:4040',
  msgaddr: 'ipc:///tmp/queue.ipc'
};

let svc = new Server(config);

let permissions: Permission[] = [['mobile', true], ['admin', true]];

svc.call('getAvailablePlans', permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info('getAvailablePlans %j', ctx);
  // http://redis.io/commands/sdiff
  redis.sdiff(list_key, entities_prefix + ctx.uid, function (err, result) {
    if (err) {
      rep([]);
    } else {
      ids2plans(result, rep);
    }
  });
});

svc.call('getJoinedPlans', permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info('getJoinedPlans %j', ctx);
  // http://redis.io/commands/smembers
  redis.smembers(entities_prefix + ctx.uid, function (err, result) {
    if (err) {
      rep([]);
    } else {
      ids2plans(result, rep);
    }
  });
});

svc.call('getPlan', permissions, (ctx: Context, rep: ResponseFunction, pid: string) => {
  log.info('getPlan %j', ctx);
  // http://redis.io/commands/hget
  redis.hget(entity_key, pid, (err, plan) => {
    if (err) {
      rep(null);
    } else {
      redis.hget('plan-joined-count', plan.id, (err, count) => {
        plan.joinedCount = count? count: 0; 
        rep(plan);
      });
    }
  });
});

svc.call('refresh', permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info('refresh %j', ctx);
  ctx.msgqueue.send(msgpack.encode({cmd: "refresh", args: null}));
  rep({status: 'okay'});
});

function ids2plans(ids: string[], rep: ResponseFunction) {
  let multi = redis.multi();
  for (let id of ids) {
    multi.hget(entity_key, id);
  }
  multi.exec(function(err, plans) {
    console.log(JSON.stringify(plans));
    for (let plan of plans) {
      multi.hget('plan-joined-count', plan.id);
    }
    multi.exec((err, counts) => {
      for (let i in counts) {
        let p = plans[i];
        p.joinedCount = counts[i]? counts[i]: 0;
      }
      rep(plans);
    });
  });
}

log.info('Start server at %s and connect to %s', config.svraddr, config.msgaddr);

svc.run();
