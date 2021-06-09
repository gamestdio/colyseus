import http from 'http';
import msgpack from 'notepack.io';

import { decode, Iterator, Schema } from '@colyseus/schema';

import Clock from '@gamestdio/timer';
import { EventEmitter } from 'events';

import { Presence } from './presence/Presence';

import { FossilDeltaSerializer } from './serializer/FossilDeltaSerializer';
import { NoneSerializer } from './serializer/NoneSerializer';
import { SchemaSerializer } from './serializer/SchemaSerializer';
import { Serializer } from './serializer/Serializer';

import { ErrorCode, getMessageBytes, Protocol } from './Protocol';
import { Deferred, spliceOne } from './Utils';

import { debugAndPrintError, debugPatch } from './Debug';
import { ServerError } from './errors/ServerError';
import { RoomListingData } from './matchmaker/drivers/Driver';
import { Client, ClientState, ISendOptions } from './transport/Transport';

const DEFAULT_PATCH_RATE = 1000 / 20; // 20fps (50ms)
const DEFAULT_SIMULATION_INTERVAL = 1000 / 60; // 60fps (16.66ms)

export const DEFAULT_SEAT_RESERVATION_TIME = Number(process.env.COLYSEUS_SEAT_RESERVATION_TIME || 15);

export type SimulationCallback = (deltaTime: number) => void;

export type RoomConstructor<T= any> = new (presence?: Presence) => Room<T>;

export interface IBroadcastOptions extends ISendOptions {
  except?: Client;
}

export enum RoomInternalState {
  CREATING = 0,
  CREATED = 1,
  DISCONNECTING = 2,
}

export abstract class Room<State= any, Metadata= any> {

  public get locked() {
    return this._locked;
  }

  public get metadata() {
    return this.listing.metadata;
  }

  public listing: RoomListingData<Metadata>;
  public clock: Clock = new Clock();

  public roomId: string;
  public roomName: string;

  public maxClients: number = Infinity;
  public patchRate: number = DEFAULT_PATCH_RATE;
  public autoDispose: boolean = true;

  public state: State;
  public presence: Presence;

  public clients: Client[] = [];
  public internalState: RoomInternalState = RoomInternalState.CREATING;

  /** @internal */
  public _events = new EventEmitter();

  // seat reservation & reconnection
  protected seatReservationTime: number = DEFAULT_SEAT_RESERVATION_TIME;
  protected reservedSeats: { [sessionId: string]: any } = {};
  protected reservedSeatTimeouts: { [sessionId: string]: NodeJS.Timer } = {};

  protected reconnections: { [sessionId: string]: Deferred } = {};

  private onMessageHandlers: {[id: string]: (client: Client, message: any) => void} = {};

  private _serializer: Serializer<State> = new NoneSerializer();
  private _afterNextPatchBroadcasts: IArguments[] = [];

  private _simulationInterval: NodeJS.Timer;
  private _patchInterval: NodeJS.Timer;

  private _locked: boolean = false;
  private _lockedExplicitly: boolean = false;
  private _maxClientsReached: boolean = false;

  // this timeout prevents rooms that are created by one process, but no client
  // ever had success joining into it on the specified interval.
  private _autoDisposeTimeout: NodeJS.Timer;

  constructor(presence?: Presence) {
    this.presence = presence;

    this._events.once('dispose', async () => {
      try {
        await this._dispose();

      } catch (e) {
        debugAndPrintError(`onDispose error: ${(e && e.message || e || 'promise rejected')}`);
      }
      this._events.emit('disconnect');
    });

    this.setPatchRate(this.patchRate);
  }

  // Optional abstract methods
  public onCreate?(options: any): void | Promise<any>;
  public onJoin?(client: Client, options?: any, auth?: any): void | Promise<any>;
  public onLeave?(client: Client, consented?: boolean): void | Promise<any>;
  public onDispose?(): void | Promise<any>;
  public onAuth(client: Client, options: any, request?: http.IncomingMessage): any | Promise<any> {
    return true;
  }

  public hasReachedMaxClients(): boolean {
    return (this.clients.length + Object.keys(this.reservedSeats).length) >= this.maxClients;
  }

  public setSeatReservationTime(seconds: number) {
    this.seatReservationTime = seconds;
    return this;
  }

  public hasReservedSeat(sessionId: string): boolean {
    return this.reservedSeats[sessionId] !== undefined;
  }

  public setSimulationInterval(onTickCallback?: SimulationCallback, delay: number = DEFAULT_SIMULATION_INTERVAL): void {
    // clear previous interval in case called setSimulationInterval more than once
    if (this._simulationInterval) { clearInterval(this._simulationInterval); }

    if (onTickCallback) {
      this._simulationInterval = setInterval(() => {
        this.clock.tick();
        onTickCallback(this.clock.deltaTime);
      }, delay);
    }
  }

  public setPatchRate(milliseconds: number): void {
    this.patchRate = milliseconds;

    // clear previous interval in case called setPatchRate more than once
    if (this._patchInterval) {
      clearInterval(this._patchInterval);
      this._patchInterval = undefined;
    }

    if (milliseconds !== null && milliseconds !== 0) {
      this._patchInterval = setInterval(() => this.broadcastPatch(), milliseconds);
    }
  }

  public setState(newState: State) {
    this.clock.start();

    if ('_definition' in newState) {
      this._serializer = new SchemaSerializer();

    } else {
      this._serializer = new FossilDeltaSerializer();
    }

    this._serializer.reset(newState);

    this.state = newState;
  }

  public async setMetadata(meta: Partial<Metadata>) {
    if (!this.listing.metadata) {
      this.listing.metadata = meta as Metadata;
    } else {
      for (const [field, value] of Object.entries(meta)) {
        if (typeof value === "object" && value !== null) {
          this.listing.metadata[field as never] = Object.assign({}, this.listing.metadata[field as never], value);
        } else {
          this.listing.metadata[field as never] = value as never;
        }
      }

      // `MongooseDriver` workaround: persit metadata mutations
      if ('markModified' in this.listing) {
        (this.listing as any).markModified('metadata');
      }
    }

    if (this.internalState === RoomInternalState.CREATED) {
      await this.listing.save();
    }
  }

  public async setPrivate(bool: boolean = true) {
    this.listing.private = bool;

    if (this.internalState === RoomInternalState.CREATED) {
      await this.listing.save();
    }
  }

  public async lock() {
    // rooms locked internally aren't explicit locks.
    this._lockedExplicitly = (arguments[0] === undefined);

    // skip if already locked.
    if (this._locked) { return; }

    this._locked = true;

    await this.listing.updateOne({
      $set: { locked: this._locked },
    });

    this._events.emit('lock');
  }

  public async unlock() {
    // only internal usage passes arguments to this function.
    if (arguments[0] === undefined) {
      this._lockedExplicitly = false;
    }

    // skip if already locked
    if (!this._locked) { return; }

    this._locked = false;

    await this.listing.updateOne({
      $set: { locked: this._locked },
    });

    this._events.emit('unlock');
  }

  public send(client: Client, type: string | number, message: any, options?: ISendOptions): void;
  public send(client: Client, message: Schema, options?: ISendOptions): void;
  public send(client: Client, messageOrType: any, messageOrOptions?: any | ISendOptions, options?: ISendOptions): void {
    console.warn('DEPRECATION WARNING: use client.send(...) instead of this.send(client, ...)');
    client.send(messageOrType, messageOrOptions, options);
  }

  public broadcast(type: string | number, message?: any, options?: IBroadcastOptions);
  public broadcast<T extends Schema>(message: T, options?: IBroadcastOptions);
  public broadcast(
    typeOrSchema: string | number | Schema,
    messageOrOptions?: any | IBroadcastOptions,
    options?: IBroadcastOptions,
  ) {
    const isSchema = (typeof(typeOrSchema) === 'object');
    const opts: IBroadcastOptions = ((isSchema) ? messageOrOptions : options);

    if (opts && opts.afterNextPatch) {
      delete opts.afterNextPatch;
      this._afterNextPatchBroadcasts.push(arguments);
      return;
    }

    if (isSchema) {
      this.broadcastMessageSchema(typeOrSchema as Schema, opts);

    } else {
      this.broadcastMessageType(typeOrSchema as string, messageOrOptions, opts);
    }
  }

  public broadcastPatch() {
    if (!this._simulationInterval) {
      this.clock.tick();
    }

    if (!this.state) {
      return false;
    }

    const hasChanges = this._serializer.applyPatches(this.clients, this.state);

    // broadcast messages enqueued for "after patch"
    this.broadcastAfterPatch();

    return hasChanges;
  }

  public onMessage<T = any>(messageType: '*', callback: (client: Client, type: string | number, message: T) => void);
  public onMessage<T = any>(messageType: string | number, callback: (client: Client, message: T) => void);
  public onMessage<T = any>(messageType: '*' | string | number, callback: (...args: any[]) => void) {
    this.onMessageHandlers[messageType] = callback;
    // returns a method to unbind the callback
    return () => delete this.onMessageHandlers[messageType];
  }

  public async disconnect(): Promise<any> {
    this.internalState = RoomInternalState.DISCONNECTING;
    await this.listing.remove();

    this.autoDispose = true;

    const delayedDisconnection = new Promise<void>((resolve) =>
      this._events.once('disconnect', () => resolve()));

    for (const reconnection of Object.values(this.reconnections)) {
      reconnection.reject();
    }

    let numClients = this.clients.length;
    if (numClients > 0) {
      // clients may have `async onLeave`, room will be disposed after they're fulfilled
      while (numClients--) {
        this._forciblyCloseClient(this.clients[numClients], Protocol.WS_CLOSE_CONSENTED);
      }
    } else {
      // no clients connected, dispose immediately.
      this._events.emit('dispose');
    }

    return await delayedDisconnection;
  }

  public async ['_onJoin'](client: Client, req?: http.IncomingMessage) {
    const sessionId = client.sessionId;

    if (this.reservedSeatTimeouts[sessionId]) {
      clearTimeout(this.reservedSeatTimeouts[sessionId]);
      delete this.reservedSeatTimeouts[sessionId];
    }

    // clear auto-dispose timeout.
    if (this._autoDisposeTimeout) {
      clearTimeout(this._autoDisposeTimeout);
      this._autoDisposeTimeout = undefined;
    }

    // get seat reservation options and clear it
    const options = this.reservedSeats[sessionId];
    delete this.reservedSeats[sessionId];

    // bind clean-up callback when client connection closes
    client.ref.once('close', this._onLeave.bind(this, client));

    this.clients.push(client);

    const reconnection = this.reconnections[sessionId];
    if (reconnection) {
      reconnection.resolve(client);

    } else {
      try {
        client.auth = await this.onAuth(client, options, req);

        if (!client.auth) {
          throw new ServerError(ErrorCode.AUTH_FAILED, 'onAuth failed');
        }

        if (this.onJoin) {
          await this.onJoin(client, options, client.auth);
        }
      } catch (e) {
        spliceOne(this.clients, this.clients.indexOf(client));

        // make sure an error code is provided.
        if (!e.code) {
          e.code = ErrorCode.APPLICATION_ERROR;
        }

        throw e;

      } finally {
        // remove seat reservation
        delete this.reservedSeats[sessionId];
      }
    }

    // emit 'join' to room handler
    this._events.emit('join', client);

    // allow client to send messages after onJoin has succeeded.
    client.ref.on('message', this._onMessage.bind(this, client));

    // confirm room id that matches the room name requested to join
    client.raw(getMessageBytes[Protocol.JOIN_ROOM](
      this._serializer.id,
      this._serializer.handshake && this._serializer.handshake(),
    ));
  }

  public allowReconnection(previousClient: Client, seconds: number = Infinity): Deferred<Client> {
    if (this.internalState === RoomInternalState.DISCONNECTING) {
      this._disposeIfEmpty(); // gracefully shutting down
      throw new Error('disconnecting');
    }

    const sessionId = previousClient.sessionId;
    this._reserveSeat(sessionId, true, seconds, true);

    // keep reconnection reference in case the user reconnects into this room.
    const reconnection = new Deferred<Client>();
    this.reconnections[sessionId] = reconnection;

    if (seconds !== Infinity) {
      // expire seat reservation after timeout
      this.reservedSeatTimeouts[sessionId] = setTimeout(() =>
        reconnection.reject(false), seconds * 1000);
    }

    const cleanup = () => {
      delete this.reservedSeats[sessionId];
      delete this.reconnections[sessionId];
      delete this.reservedSeatTimeouts[sessionId];
    };

    reconnection.
      then((newClient) => {
        newClient.auth = previousClient.auth;
        previousClient.ref = newClient.ref; // swap "ref" for convenience
        previousClient.state = ClientState.RECONNECTED;
        clearTimeout(this.reservedSeatTimeouts[sessionId]);
        cleanup();
      }).
      catch(() => {
        cleanup();
        this.resetAutoDisposeTimeout();
      });

    return reconnection;
  }

  protected resetAutoDisposeTimeout(timeoutInSeconds: number = 1) {
    clearTimeout(this._autoDisposeTimeout);

    if (!this.autoDispose) {
      return;
    }

    this._autoDisposeTimeout = setTimeout(() => {
      this._autoDisposeTimeout = undefined;
      this._disposeIfEmpty();
    }, timeoutInSeconds * 1000);
  }

  private broadcastMessageSchema<T extends Schema>(message: T, options: IBroadcastOptions = {}) {
    const encodedMessage = getMessageBytes[Protocol.ROOM_DATA_SCHEMA](message);

    let numClients = this.clients.length;
    while (numClients--) {
      const client = this.clients[numClients];

      if (options.except !== client) {
        client.enqueueRaw(encodedMessage);
      }
    }
  }

  private broadcastMessageType(type: string, message?: any, options: IBroadcastOptions = {}) {
    const encodedMessage = getMessageBytes[Protocol.ROOM_DATA](type, message);

    let numClients = this.clients.length;
    while (numClients--) {
      const client = this.clients[numClients];

      if (options.except !== client) {
        client.enqueueRaw(encodedMessage);
      }
    }
  }

  private sendFullState(client: Client): void {
    client.enqueueRaw(getMessageBytes[Protocol.ROOM_STATE](this._serializer.getFullState(client)));
  }

  private broadcastAfterPatch() {
    const length = this._afterNextPatchBroadcasts.length;

    if (length > 0) {
      for (let i = 0; i < length; i++) {
        this.broadcast.apply(this, this._afterNextPatchBroadcasts[i]);
      }

      // new messages may have been added in the meantime,
      // let's splice the ones that have been processed
      this._afterNextPatchBroadcasts.splice(0, length);
    }
  }

  private async _reserveSeat(
    sessionId: string,
    joinOptions: any = true,
    seconds: number = this.seatReservationTime,
    allowReconnection: boolean = false,
  ) {
    if (!allowReconnection && this.hasReachedMaxClients()) {
      return false;
    }

    this.reservedSeats[sessionId] = joinOptions;

    if (!allowReconnection) {
      await this._incrementClientCount();

      this.reservedSeatTimeouts[sessionId] = setTimeout(async () => {
        delete this.reservedSeats[sessionId];
        delete this.reservedSeatTimeouts[sessionId];
        await this._decrementClientCount();
      }, seconds * 1000);

      this.resetAutoDisposeTimeout(seconds);
    }

    return true;
  }

  private _disposeIfEmpty() {
    const willDispose = (
      this.autoDispose &&
      this._autoDisposeTimeout === undefined &&
      this.clients.length === 0 &&
      Object.keys(this.reservedSeats).length === 0
    );

    if (willDispose) {
      this._events.emit('dispose');
    }

    return willDispose;
  }

  private async _dispose(): Promise<any> {
    let userReturnData;

    if (this.onDispose) {
      userReturnData = this.onDispose();
    }

    if (this._patchInterval) {
      clearInterval(this._patchInterval);
      this._patchInterval = undefined;
    }

    if (this._simulationInterval) {
      clearInterval(this._simulationInterval);
      this._simulationInterval = undefined;
    }

    if (this._autoDisposeTimeout) {
      clearInterval(this._autoDisposeTimeout);
      this._autoDisposeTimeout = undefined;
    }

    // clear all timeouts/intervals + force to stop ticking
    this.clock.clear();
    this.clock.stop();

    return await (userReturnData || Promise.resolve());
  }

  private _onMessage(client: Client, bytes: number[]) {
    // skip if client is on LEAVING state.
    if (client.state === ClientState.LEAVING) { return; }

    const it: Iterator = { offset: 0 };
    const code = decode.uint8(bytes, it);

    if (!bytes) {
      debugAndPrintError(`${this.roomName} (${this.roomId}), couldn't decode message: ${bytes}`);
      return;
    }

    if (code === Protocol.ROOM_DATA) {
      const messageType = (decode.stringCheck(bytes, it))
        ? decode.string(bytes, it)
        : decode.number(bytes, it);

      let message;
      try {
        message = (bytes.length > it.offset)
        ? msgpack.decode(bytes.slice(it.offset, bytes.length))
        : undefined;
      } catch (e) {
        debugAndPrintError(e);
        return;
      }

      if (this.onMessageHandlers[messageType]) {
        this.onMessageHandlers[messageType](client, message);

      } else if (this.onMessageHandlers['*']) {
        (this.onMessageHandlers['*'] as any)(client, messageType, message);

      } else {
        debugAndPrintError(`onMessage for "${messageType}" not registered.`);
      }

    } else if (code === Protocol.JOIN_ROOM) {
      // join room has been acknowledged by the client
      client.state = ClientState.JOINED;

      // send current state when new client joins the room
      if (this.state) {
        this.sendFullState(client);
      }

      // dequeue messages sent before client has joined effectively (on user-defined `onJoin`)
      if (client._enqueuedMessages.length > 0) {
        client._enqueuedMessages.forEach((enqueued) => client.raw(enqueued));
      }
      delete client._enqueuedMessages;

    } else if (code === Protocol.LEAVE_ROOM) {
      this._forciblyCloseClient(client, Protocol.WS_CLOSE_CONSENTED);
    }

  }

  private _forciblyCloseClient(client: Client, closeCode: number) {
    // stop receiving messages from this client
    client.ref.removeAllListeners('message');

    // prevent "onLeave" from being called twice if player asks to leave
    const closeListeners: any[] = client.ref.listeners('close');
    if (closeListeners.length >= 2) {
      client.ref.removeListener('close', closeListeners[1]);
    }

    // only effectively close connection when "onLeave" is fulfilled
    this._onLeave(client, closeCode).then(() => client.leave(Protocol.WS_CLOSE_NORMAL));
  }

  private async _onLeave(client: Client, code?: number): Promise<any> {
    const success = spliceOne(this.clients, this.clients.indexOf(client));

    // call 'onLeave' method only if the client has been successfully accepted.
    if (success && this.onLeave) {
      try {
        client.state = ClientState.LEAVING;
        await this.onLeave(client, (code === Protocol.WS_CLOSE_CONSENTED));

      } catch (e) {
        debugAndPrintError(`onLeave error: ${(e && e.message || e || 'promise rejected')}`);
      }
    }

    if (client.state !== ClientState.RECONNECTED) {
      // try to dispose immediatelly if client reconnection isn't set up.
      const willDispose = await this._decrementClientCount();

      this._events.emit('leave', client, willDispose);
    }
  }

  private async _incrementClientCount() {
    // lock automatically when maxClients is reached
    if (!this._locked && this.hasReachedMaxClients()) {
      this._maxClientsReached = true;
      this.lock.call(this, true);
    }

    await this.listing.updateOne({
      $inc: { clients: 1 },
      $set: { locked: this._locked },
    });
  }

  private async _decrementClientCount() {
    const willDispose = this._disposeIfEmpty();

    if (this.internalState === RoomInternalState.DISCONNECTING) {
      return;
    }

    // unlock if room is available for new connections
    if (!willDispose) {
      if (this._maxClientsReached && !this._lockedExplicitly) {
        this._maxClientsReached = false;
        this.unlock.call(this, true);
      }

      // update room listing cache
      await this.listing.updateOne({
        $inc: { clients: -1 },
        $set: { locked: this._locked },
      });
    }

    return willDispose;
  }

}
