import { Server, Config, Context, ResponseFunction, Permission } from 'hive-server';
import * as Redis from "redis";
import * as nanomsg from 'nanomsg';
import * as msgpack from 'msgpack-lite';
import * as bunyan from 'bunyan';
import * as hostmap from './hostmap';

let log = bunyan.createLogger({
  name: 'plan-server',
  streams: [
    {
      level: 'info',
      path: '/var/log/plan-server-info.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1d',   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: 'error',
      path: '/var/log/plan-server-error.log',  // log ERROR and above to a file
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
  svraddr: hostmap.default["plan"],
  msgaddr: 'ipc:///tmp/plan.ipc'
};

let svc = new Server(config);

let permissions: Permission[] = [['mobile', true], ['admin', true]];

svc.call('getAvailablePlans', permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info('getAvailablePlans uid: %s', ctx.uid);
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
  log.info('getJoinedPlans uid: %s', ctx.uid);
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
  log.info('getPlan uid: %s, pid: %s', ctx.uid, pid);
  // http://redis.io/commands/hget
  redis.hget(entity_key, pid, (err, plan) => {
    if (err) {
      rep(null);
    } else if (plan) {
      redis.hget('plan-joined-count', plan.id, (err, count) => {
        plan.joinedCount = count? count: 0; 
        rep(plan);
      });
    } else {
      rep(null);
    }
  });
});

svc.call('refresh', permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info('refresh uid: %s', ctx.uid);
  ctx.msgqueue.send(msgpack.encode({cmd: "refresh", args: null}));
  rep({status: 'okay'});
});

function ids2plans(ids: string[], rep: ResponseFunction) {
  let multi = redis.multi();
  for (let id of ids) {
    multi.hget(entity_key, id);
  }
  multi.exec(function(err, planstrs) {
    if (err) {
      rep([]);
    } else {
      let plans = [];
      for (let planstr of planstrs) {
        let plan = JSON.parse(planstr);
        multi.hget('plan-joined-count', plan.id);
        plans.push(plan);
      }
      multi.exec((err, counts) => {
        for (let i in counts) {
          let p = plans[i];
          p.joined_count = counts[i]? counts[i]: 0;
        }
        rep(plans);
      });
    }
  });
}

log.info('Start server at %s and connect to %s', config.svraddr, config.msgaddr);

svc.run();
