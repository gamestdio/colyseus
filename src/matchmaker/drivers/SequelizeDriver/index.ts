import { MatchMakerDriver, QueryHelpers, RoomListingData } from '../Driver';
import { Sequelize, Options, Filterable, SyncOptions } from 'sequelize';
import { RoomCache, RoomCacheSchema } from './RoomCache';
import { Query } from './Query';

export class SequelizeDriver<TRecord extends {} = any, TResult = unknown[]>
  implements MatchMakerDriver {
  public readonly connection: Sequelize;

  public constructor(uri: string = 'sqlite::memory:', options?: Options) {
    this.connection = new Sequelize(uri, options);

    RoomCache.init(RoomCacheSchema, {
      sequelize: this.connection,
      modelName: 'RoomCache',
      tableName: 'room_caches',
      indexes: [{ fields: ['name', 'locked'] }, { fields: ['locked'] }],
      underscored: true,
    });
  }

  public createInstance(initialValues: any = {}) {
    return (new RoomCache(initialValues) as any) as RoomListingData;
  }

  public async find(where?: Filterable<RoomCache['_attributes']>['where']) {
    const result = await RoomCache.findAll({
      attributes: [
        'clients',
        'createdAt',
        'locked',
        'maxClients',
        'metadata',
        'name',
        'roomId',
      ],
      where,
    });

    return (result as any) as RoomListingData[];
  }

  public findOne(conditions: any) {
    return (new Query<RoomListingData>(
      conditions
    ) as any) as QueryHelpers<RoomListingData>;
  }
}
