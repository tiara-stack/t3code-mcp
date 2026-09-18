#!/usr/bin/env node

import { NodeRuntime } from "@effect/platform-node";
import * as Layer from "effect/Layer";
import { serverLayer } from "./server";

NodeRuntime.runMain(Layer.launch(serverLayer));
