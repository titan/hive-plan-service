import { Server, ServerContext, ServerFunction, CmdPacket, Permission, waitingAsync } from "hive-service";
import * as bunyan from "bunyan";
import { verify, numberVerifier } from "hive-verify";

const log = bunyan.createLogger({
  name: "plan-server",
  streams: [
    {
      level: "info",
      path: "/var/log/plan-server-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/plan-server-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

const allowAll: Permission[] = [["mobile", true], ["admin", true]];
const adminOnly: Permission[] = [["mobile", false], ["admin", true]];

export const server = new Server();

server.callAsync("getPlans", allowAll, "获取所有计划", "获取所有计划", async (ctx: ServerContext) => {
  log.info(`getPlans`);
  const plans = await ctx.cache.hvalsAsync("plan-entities");
  return { code: 200, data: plans }
});

server.callAsync("getPlanGroups", allowAll, "获取所有计划套餐", "获取所有计划套餐", async (ctx: ServerContext) => {
  log.info(`getPlanGroups`);
  const groups = await ctx.cache.hvalsAsync("plan-group-entities");
  return { code: 200, data: groups };
});

server.callAsync("increaseJoinedCount", adminOnly, "增加已加入车辆数量", "增加已加入车辆数量", async (ctx: ServerContext) => {
  log.info("increaseJoinedCount uid: %s", ctx.uid);
  await ctx.cache.incrAsync("plan-joined-count");
});

server.callAsync("decreaseJoinedCount", adminOnly, "减少已加入车辆数量", "减少已加入车辆数量", async (ctx: ServerContext) => {
  log.info("decreaseJoinedCount uid: %s", ctx.uid);
  await ctx.cache.decrAsync("plan-joined-count");
});

server.callAsync("setJoinedCount", adminOnly, "设置计划加入车辆数", "可以以数组的方式批量设置计划加入的车辆数。", async (ctx: ServerContext, count: number) => {
  log.info("setJoinedCounts uid: %s, count: %d", ctx.uid, count);
  try {
    verify([numberVerifier("count", count)]);
  } catch (e) {
    return { code: 400, msg: e.message };
  }
  await ctx.cache.set("plan-joined-count", count);
  return { code: 200, data: "SUCCESS" };
});

server.callAsync("refresh", adminOnly, "", "", async (ctx: ServerContext) => {
  log.info("refresh uid: %s", ctx.uid);
  const pkt: CmdPacket = { cmd: "refresh", args: [] };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});
