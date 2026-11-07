import { parentPort } from "node:worker_threads"
import { codeWorkerUrl } from "../../src/cluster/code-runtime-worker"

const port = parentPort
if (port !== null) port.once("message", message => port.postMessage({ message, worker: codeWorkerUrl.protocol }))
