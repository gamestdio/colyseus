import { Model, ModelAttributes, DataTypes, Optional } from 'sequelize';
import { ulid } from 'ulid';

export interface Dictionary<T> {
  [key: string]: T | undefined;
}

export interface RoomCacheAttributes {
  id: string;
  uuid: string;
  clients: number;
  maxClients: number;
  metadata: Dictionary<unknown> | null;
  locked: boolean;
  private: boolean;
  unlisted: boolean;
  name: string;
  processId: string;
  roomId: string;
}

export interface RoomCacheCreationAttributes
  extends Optional<RoomCacheAttributes, 'id' | 'uuid'> {}

interface UpdateOperation {
  $set?: RoomCacheCreationAttributes;
  $inc?: RoomCacheCreationAttributes;
}

export class RoomCache
  extends Model<RoomCacheAttributes, RoomCacheCreationAttributes>
  implements RoomCacheAttributes {
  public id!: string;
  public uuid!: string;
  public clients!: number;
  public maxClients!: number;
  public metadata!: Dictionary<string> | null;
  public locked!: boolean;
  public private!: boolean;
  public unlisted!: boolean;
  public name!: string;
  public processId!: string;
  public roomId!: string;

  public readonly createdAt!: Date;
  public readonly updatedAt!: Date;

  public toJSON() {
    return {
      clients: this.clients,
      createdAt: this.createdAt,
      maxClients: this.maxClients,
      metadata: this.metadata,
      name: this.name,
      processId: this.processId,
      roomId: this.roomId,
    };
  }

  public async updateOne(operations: UpdateOperation) {
    let changes = 0;

    if (operations.$set) {
      for (const [field, value] of Object.entries(operations.$set)) {
        this[field as keyof this] = value;
        changes += 1;
      }
    }

    if (operations.$inc) {
      for (const [field, value] of Object.entries(operations.$inc)) {
        this[field as keyof this] += value;
        changes += 1;
      }
    }

    if (changes) {
      await this.save();
    }

    return this;
  }

  public async remove() {
    if (!this.isNewRecord) {
      await this.destroy();
    }
  }
}

export const RoomCacheSchema: ModelAttributes<
  RoomCache,
  RoomCache['_attributes']
> = {
  id: {
    type: DataTypes.BIGINT.UNSIGNED,
    autoIncrement: true,
    primaryKey: true,
  },
  uuid: {
    type: DataTypes.CHAR(26).BINARY,
    defaultValue() {
      return ulid().toLowerCase();
    },
    allowNull: false,
    unique: true,
  },
  name: {
    allowNull: false,
    type: DataTypes.STRING,
  },
  processId: {
    allowNull: false,
    type: DataTypes.STRING,
  },
  roomId: {
    allowNull: false,
    type: DataTypes.STRING,
  },
  clients: {
    allowNull: true,
    type: DataTypes.INTEGER.UNSIGNED,
    defaultValue: 0,
  },
  maxClients: {
    allowNull: true,
    type: DataTypes.INTEGER,
    defaultValue: null,
    get(): number {
      const rawValue = this.getDataValue('maxClients');
      return rawValue === null ? Infinity : rawValue;
    },
    set(value: number) {
      this.setDataValue(
        'maxClients',
        value === Infinity || value < 0 ? null : value
      );
    },
  },
  metadata: {
    allowNull: true,
    type: DataTypes.JSON,
  },
  locked: {
    allowNull: true,
    defaultValue: false,
    type: DataTypes.BOOLEAN,
  },
  private: {
    allowNull: true,
    defaultValue: false,
    type: DataTypes.BOOLEAN,
  },
  unlisted: {
    allowNull: true,
    defaultValue: false,
    type: DataTypes.BOOLEAN,
    comment:
      'Used for default LobbyRoom (prevent from showing up on room listing)',
  },
};
