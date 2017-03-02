import { Server, ServerContext, ServerFunction, CmdPacket, Permission, waitingAsync, msgpack_decode_async } from "hive-service";
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
  const data = [];
  for (const plan of plans) {
    data.push(await msgpack_decode_async(plan));
  }
  return { code: 200, data: data };
});

server.callAsync("getPlanGroups", allowAll, "获取所有计划套餐", "获取所有计划套餐", async (ctx: ServerContext) => {
  log.info(`getPlanGroups`);
  const groups = await ctx.cache.hvalsAsync("plan-group-entities");
  const data = [];
  for (const group of groups) {
    data.push(await msgpack_decode_async(group));
  }
  return { code: 200, data: data };
});

server.callAsync("increaseJoinedCount", adminOnly, "增加已加入车辆数量", "增加已加入车辆数量", async (ctx: ServerContext) => {
  log.info("increaseJoinedCount uid: %s", ctx.uid);
  const count = await ctx.cache.incrAsync("plan-joined-count");
  return { code: 200, data: count };
});

server.callAsync("decreaseJoinedCount", adminOnly, "减少已加入车辆数量", "减少已加入车辆数量", async (ctx: ServerContext) => {
  log.info("decreaseJoinedCount uid: %s", ctx.uid);
  const count = await ctx.cache.decrAsync("plan-joined-count");
  return { code: 200, data: count };
});

server.callAsync("setJoinedCount", adminOnly, "设置计划加入车辆数", "设置计划加入的车辆数", async (ctx: ServerContext, count: number) => {
  log.info("setJoinedCounts uid: %s, count: %d", ctx.uid, count);
  try {
    verify([numberVerifier("count", count)]);
  } catch (e) {
    return { code: 400, msg: e.message };
  }
  await ctx.cache.setAsync("plan-joined-count", count);
  return { code: 200, data: count };
});

server.callAsync("getJoinedCount", allowAll, "获得计划加入车辆数", "获得计划加入车辆数", async (ctx: ServerContext) => {
  log.info("getJoinedCounts uid: %s", ctx.uid);
  const count = await ctx.cache.getAsync("plan-joined-count");
  if (count) {
    const cnt = count.toString();
    return { code: 200, data: parseInt(cnt) };
  } else {
    return { code: 200, data: 0 };
  }
});

server.callAsync("refresh", adminOnly, "", "", async (ctx: ServerContext) => {
  log.info("refresh uid: %s", ctx.uid);
  const pkt: CmdPacket = { cmd: "refresh", args: [] };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});
