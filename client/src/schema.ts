/**
 * Schema decorator metadata collection.
 *
 * Collects field names and types from decorated classes, then
 * generates a proto3 schema string for broker registration.
 *
 * Usage:
 *   @Schema()
 *   class Order {
 *     @Field() id!: string;
 *     @Field({ type: "int32" }) qty!: number;
 *   }
 */

export type ProtoType =
  | "string"
  | "int32"
  | "int64"
  | "uint32"
  | "uint64"
  | "float"
  | "double"
  | "bool"
  | "bytes";

interface FieldMeta {
  name: string;
  protoType: ProtoType;
  number: number;
}

const FIELD_STORE = new Map<Function, FieldMeta[]>();

/** Marks a class as a schema definition (TC39 stage 3 decorator). */
export function Schema() {
  return function <T extends new (...args: any[]) => any>(
    target: T,
    _ctx: ClassDecoratorContext
  ): T {
    if (!FIELD_STORE.has(target)) {
      FIELD_STORE.set(target, []);
    }
    return target;
  };
}

/** Marks a property as a schema field (TC39 stage 3 decorator). */
export function Field(opts?: { type?: ProtoType }) {
  return function (_value: undefined, ctx: ClassFieldDecoratorContext) {
    const name = String(ctx.name);

    // Defer field registration until the class is fully defined.
    ctx.addInitializer(function (this: any) {
      const ctor = this.constructor;
      const fields = FIELD_STORE.get(ctor) ?? [];

      // Avoid duplicate registrations from multiple instances
      if (fields.some((f) => f.name === name)) return;

      fields.push({
        name,
        protoType: opts?.type ?? "string",
        number: fields.length + 1,
      });
      FIELD_STORE.set(ctor, fields);
    });
  };
}

/** Returns the collected field metadata for a schema class. */
export function getFields(schema: Function): FieldMeta[] {
  return FIELD_STORE.get(schema) ?? [];
}

/**
 * Generates a proto3 schema string from a decorated class.
 *
 * Example output:
 *   syntax = "proto3"; message Order { string id = 1; int32 qty = 2; }
 */
export function toProto(schema: Function): string {
  // Force field registration by creating an instance
  if (getFields(schema).length === 0) {
    try {
      new (schema as any)();
    } catch {
      // Constructor may throw — fields are registered via addInitializer
    }
  }

  const fields = getFields(schema);
  if (fields.length === 0) {
    throw new Error(
      `Schema '${schema.name}' has no @Field() decorators — cannot generate proto`
    );
  }

  const body = fields
    .map((f) => `${f.protoType} ${f.name} = ${f.number};`)
    .join(" ");

  return `syntax = "proto3"; message ${schema.name} { ${body} }`;
}
