import assert, {AssertionError, fail} from "assert";
import express from "express";
import rateLimit from "express-rate-limit";

import {createServer} from "http";
import * as httpClient from "httpie";
import { Server } from "../src";

describe("ExpressServer", () => {

  const app = express();
  app.use(rateLimit({
    headers: true,
    max: 1,
    message: "You have exceeded the requests limit!",
    windowMs: 5 * 1000, // 5 seconds
  }));

  const server = new Server({
    server: createServer(app),
  });

  // bind & unbind server
  before(async () => new Promise((resolve) => {
    // listen for testing
    server.listen(8567, undefined, undefined, resolve);
  }));

  after(() => server.transport.shutdown());

  describe("Middlewares", () => {

    it("should rate limit after 1 successful call", async () => {

      const responseOk = await httpClient.get("http://localhost:8567/matchmake/");
      assert.ok(responseOk.data);
      assert.deepStrictEqual(responseOk.data, []);

      try {
        await httpClient.get("http://localhost:8567/matchmake/");
        fail();
      } catch (e) {
        if (e instanceof AssertionError) {
          fail("Middlewares not working - request is NOT rate limited.");
        } else {
          assert.strictEqual(e.statusCode, 429);
          assert.strictEqual(e.statusMessage, "Too Many Requests");
          assert.strictEqual(e.data, "You have exceeded the requests limit!");
        }
      }

    });

  });

});
