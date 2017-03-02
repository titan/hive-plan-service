import { Service, Server, Processor, Config } from "hive-service";
import { server } from "./plan-server";
import { processor } from "./plan-processor";

const config: Config = {
  modname: "plan",
  serveraddr: process.env["PLAN"],
  queueaddr: "ipc:///tmp/plan.ipc",
  cachehost: process.env["CACHE_HOST"],
  dbhost: process.env["DB_HOST"],
  dbuser: process.env["DB_USER"],
  dbport: process.env["DB_PORT"],
  database: process.env["DB_NAME"],
  dbpasswd: process.env["DB_PASSWORD"],
};

const svc: Service = new Service(config);

svc.registerServer(server);
svc.registerProcessor(processor);

svc.run();
