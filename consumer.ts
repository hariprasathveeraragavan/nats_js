import {
    AckPolicy,
    connect,
    millis,
    nuid,
  } from "nats";
import sqlupdater from "./sqlupdater";
  
  
  const servers = "nats://192.168.126.246:4222";
  
  (async () => {
  
  const nc = await connect({
    servers: servers.split(","),
  });
  
  
  const js = nc.jetstream();
  
  const jsm = await js.jetstreamManager();
  
  
  
  const subj = nuid.next();
  const name = `EVENTS_${subj}`;
  await jsm.streams.add({
    name,
    subjects: [`${subj}.>`],
  });
  
  
  let ci = await jsm.consumers.add(name, { ack_policy: AckPolicy.None });
  
  const c = await js.consumers.get(name, ci.name);
  
  await jsm.consumers.add(name, {
    ack_policy: AckPolicy.Explicit,
    durable_name: "A",
  });
  
  const c2 = await js.consumers.get(name, "A");
  
  
  let iter = await c2.fetch({ max_messages: 3 });
  for await (const m of iter) {
    console.log(m.subject);
    console.log(m.data.toString());
    const data = JSON.parse(m.data.toString());
    await sqlupdater(data);
    m.ack();
  }
  
  await c2.delete();
  
  await nc.drain();

})();