import type {
  PutEventsRequestEntry,
  PutEventsResponse,
} from '@aws-sdk/client-eventbridge';
import Ajv from 'ajv';
import type { EventBridgeEvent } from 'aws-lambda';
import { FromSchema, JSONSchema } from 'json-schema-to-ts';

import { Bus } from './Bus';

const ajv = new Ajv();

export class Event<N extends string, S extends JSONSchema> {
  private _name: N;
  private _source: string;
  private _bus: Bus;
  private _schema: S;
  private _validate: Ajv.ValidateFunction;
  private _pattern: { 'detail-type': [N]; source: string[] };

  constructor({
    name,
    source,
    bus,
    schema,
  }: {
    name: N;
    source: string;
    bus: Bus;
    schema: S;
  }) {
    this._name = name;
    this._source = source;
    this._bus = bus;
    this._schema = schema;
    this._validate = ajv.compile(schema);
    this._pattern = { source: [source], 'detail-type': [name] };
  }

  get name(): N {
    return this._name;
  }

  get source(): string {
    return this._source;
  }

  get bus(): Bus {
    return this._bus;
  }

  get schema(): S {
    return this._schema;
  }

  get pattern(): { 'detail-type': [N]; source: string[] } {
    return this._pattern;
  }

  get publishedEventSchema(): {
    type: 'object';
    properties: {
      source: { const: string };
      'detail-type': { const: N };
      detail: S;
    };
    required: ['source', 'detail-type', 'detail'];
  } {
    return {
      type: 'object',
      properties: {
        source: { const: this._source },
        'detail-type': { const: this._name },
        detail: this._schema,
      },
      required: ['source', 'detail-type', 'detail'],
    };
  }

  create(event: FromSchema<S>): PutEventsRequestEntry {
    return {
      Source: this._source,
      DetailType: this._name,
      Detail: JSON.stringify(event),
    };
  }

  validate(event: FromSchema<S>): void {
    if (!this._validate(event)) {
      throw new Error(
        `Event validation failed: ${JSON.stringify(this._validate.errors)}`,
      );
    }
  }

  async publish(event: FromSchema<S>): Promise<PutEventsResponse> {
    return this._bus.put([this.create(event)]);
  }
}

type GenericEvent = Event<string, JSONSchema>;

export type PublishedEvent<Event extends GenericEvent> = EventBridgeEvent<
  Event['name'],
  FromSchema<Event['schema']>
>;
