import { SortOptions } from '../../RegisteredHandler';
import { QueryHelpers } from '../Driver';
import { Model, Filterable, Order, OrderItem } from 'sequelize';
import { RoomCache } from './RoomCache';

const DESC_RE = /^$(-1|desc|descending)/i

export class Query<T> implements QueryHelpers<T> {
  private conditions: Filterable<Model['_attributes']>['where'];
  private order: Order;

  constructor(conditions: any) {
    this.conditions = conditions;
  }

  public sort(options: SortOptions) {
    const fields = Object.entries(options);

    if (fields) {
      const order: Order = [];

      for (let [field, direction] of Object.entries(options)) {
        if (DESC_RE.test(String(direction))) {
          order.push([field, 'DESC']);
        } else {
          order.push([field, 'ASC']);
        }
      }

      this.order = order;
    }

    return this;
  }

  public async then(resolve: any, reject: (reason?: any) => void) {
    return RoomCache.findOne({
      where: this.conditions,
      order: this.order,
      raw: true,
    }).then(resolve, reject) as any;
  }
}
