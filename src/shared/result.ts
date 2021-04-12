
export class Result<T> {
  private constructor(private readonly _value?: T, private readonly _promise?: Promise<T>) { }

  static withValue<T>(value: T): Result<T> {
    return new Result<T>(value);
  };

  static withPromise<T>(promise: Promise<T>): Result<T> {
    return new Result<T>(undefined, promise);
  };

  get hasValue(): boolean {
    return this._promise === undefined;
  }

  get value(): T {
    if (this._promise !== undefined) {
      throw new Error("result does not have value, use promise instead");
    }
    return this._value as T;
  }

  get promise(): Promise<T> {
    if (this._promise === undefined) {
      throw new Error("result does not have promise, use value instead");
    }
    return this._promise;
  }

  toPromise(): Promise<T> {
    if (this.hasValue) {
      return Promise.resolve(this.value);
    }
    else {
      return this.promise;
    }
  }

  transform<R>(transformFunction: (t: T) => R | Result<R>): Result<R> {
    if (this.hasValue) {
      const valueOrResult = transformFunction(this.value);
      return valueOrResult instanceof Result ? valueOrResult : Result.withValue(valueOrResult);
    }
    else {
      return Result.withPromise(this.promise.then(result => {
        const valueOrResult = transformFunction(result);
        if (valueOrResult instanceof Result) {
          if (valueOrResult.hasValue) {
            return valueOrResult.value;
          }
          else {
            return valueOrResult.promise;
          }
        }
        else {
          return valueOrResult;
        }
      }));
    }
  }
}
