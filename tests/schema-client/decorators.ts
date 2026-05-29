/**
 * TypeScript decorators that map plain classes to Protobuf schemas.
 *
 * Usage:
 *   @ProtoMessage("mypackage", "MyMessage")
 *   class MyMessage {
 *     @ProtoField(1, "string")
 *     name: string;
 *
 *     @ProtoField(2, "int32")
 *     age: number;
 *   }
 *
 * The wrapper reads the decorator metadata at runtime, generates
 * the `.proto` source internally, and sends it to the broker.
 * The client never touches raw Protobuf syntax.
 */

// ── Metadata symbols (hidden from consumer code) ────────────────────

const PROTO_MESSAGE_META = Symbol("proto:message");
const PROTO_FIELD_META = Symbol("proto:fields");

// ── Public types ────────────────────────────────────────────────────

export interface ProtoMessageMeta {
  packageName: string;
  messageName: string;
}

export interface ProtoFieldMeta {
  fieldNumber: number;
  protoType: string;
}

// ── Decorators ──────────────────────────────────────────────────────

/**
 * Marks a class as a Protobuf message.
 *
 * @param packageName - The proto package (e.g. "test", "myapp.events").
 * @param messageName - The message name (e.g. "Point", "UserCreated").
 */
export function ProtoMessage(packageName: string, messageName: string) {
  return function (target: any) {
    target[PROTO_MESSAGE_META] = { packageName, messageName } as ProtoMessageMeta;
  };
}

/**
 * Marks a class property as a Protobuf field.
 *
 * @param fieldNumber - The proto field number (must be unique per message).
 * @param protoType   - The proto scalar type ("int32", "string", "bool", "double", "float",
 *                      "int64", "uint32", "uint64", "bytes", etc.).
 */
export function ProtoField(fieldNumber: number, protoType: string) {
  return function (target: any, propertyKey: string) {
    const ctor = target.constructor;
    if (!ctor[PROTO_FIELD_META]) {
      ctor[PROTO_FIELD_META] = new Map<string, ProtoFieldMeta>();
    }
    (ctor[PROTO_FIELD_META] as Map<string, ProtoFieldMeta>).set(propertyKey, {
      fieldNumber,
      protoType,
    });
  };
}

// ── Proto source generator ──────────────────────────────────────────

/**
 * Reads decorator metadata from a class and generates the `.proto` source
 * and fully-qualified message name automatically.
 *
 * @throws If the class is missing `@ProtoMessage` or has no `@ProtoField` decorators.
 */
export function generateProtoFromClass(cls: any): {
  protoSrc: string;
  fullMessageName: string;
} {
  const meta: ProtoMessageMeta | undefined = cls[PROTO_MESSAGE_META];
  if (!meta) {
    throw new Error(
      `Class "${cls.name}" is missing the @ProtoMessage decorator.`
    );
  }

  const fields: Map<string, ProtoFieldMeta> | undefined = cls[PROTO_FIELD_META];
  if (!fields || fields.size === 0) {
    throw new Error(
      `Class "${cls.name}" has no @ProtoField decorators. At least one field is required.`
    );
  }

  // Sort fields by field number for deterministic output
  const sorted = [...fields.entries()].sort(
    ([, a], [, b]) => a.fieldNumber - b.fieldNumber
  );

  let proto = `syntax = "proto3";\n`;
  if (meta.packageName) {
    proto += `package ${meta.packageName};\n`;
  }
  proto += `\nmessage ${meta.messageName} {\n`;

  for (const [name, field] of sorted) {
    proto += `  ${field.protoType} ${name} = ${field.fieldNumber};\n`;
  }

  proto += `}\n`;

  const fullName = meta.packageName
    ? `${meta.packageName}.${meta.messageName}`
    : meta.messageName;

  return { protoSrc: proto, fullMessageName: fullName };
}
