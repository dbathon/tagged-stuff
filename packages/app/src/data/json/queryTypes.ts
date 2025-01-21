import { type JsonPathKey } from "./jsonEvents";

export type JsonPrimitiveWithRange = number | string;
export type JsonPrimitive = JsonPrimitiveWithRange | boolean | null;

export type PathArray = [string, ...JsonPathKey[]];

type RangeOperator = "<=" | ">=" | "<" | ">";
export type Operator = "=" | RangeOperator | "in" | "is" | "match";

type OperatorArgument<O extends Operator> = O extends "="
  ? JsonPrimitive
  : O extends RangeOperator
    ? JsonPrimitiveWithRange
    : O extends "in"
      ? JsonPrimitive[]
      : O extends "is"
        ? "string" | "number" | "boolean"
        : O extends "match"
          ? (value: JsonPrimitive) => boolean
          : never;

type OperatorAndArgument<O extends Operator = Operator> = O extends any ? [O, OperatorArgument<O>] : never;

type PathExpressionAndOperatorAndArgument<O extends Operator = Operator> = O extends any
  ? [`${string} ${O}`, OperatorArgument<O>]
  : never;

export type FilterCondition =
  | [...PathArray, ...OperatorAndArgument]
  | PathExpressionAndOperatorAndArgument
  | FilterCondition[]
  | ["or", FilterCondition, ...FilterCondition[]];

export type Path = PathArray | string;

export type ProjectionType = "onlyId" | Path[] | undefined;

export interface CountParameters {
  table: string;
  filter?: FilterCondition;
  extraFilter?: (json: object) => boolean;
}

export interface QueryParameters extends CountParameters {
  // TODO asc/desc...
  orderBy?: Path[];
  limit?: number;
  offset?: number;
}

export type QueryResult<P extends ProjectionType, T = object> = P extends "onlyId"
  ? number[]
  : P extends Path[]
    ? (JsonPrimitive | undefined)[][]
    : P extends undefined
      ? T[]
      : never;
