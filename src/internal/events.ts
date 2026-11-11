import { Predicate, type SchemaAST } from "effect"

/**
 * The struct an event is stored as: the event's own schema for a tagged struct,
 * the fields of a class made with `Schema.TaggedClass` or `Schema.Class`, which
 * is encoded as that struct, or `undefined` for anything else.
 */
export const structOf = (ast: SchemaAST.AST): SchemaAST.Objects | undefined => {
  if (ast._tag === "Objects") return ast
  if (ast._tag !== "Declaration") return undefined
  const encoded = ast.encoding?.[0]?.to
  return encoded?._tag === "Objects" ? encoded : undefined
}

/** The tag of a struct, from a `_tag` property holding a string literal. */
export const tagOf = (ast: SchemaAST.Objects): string | undefined => {
  const tag = ast.propertySignatures.find(({ name }) => name === "_tag")?.type
  return tag?._tag === "Literal" && Predicate.isString(tag.literal) ? tag.literal : undefined
}

/**
 * The members of a union of events with their tags, in order. A member that is
 * neither a tagged struct nor a tagged class comes with an `undefined` tag, for the
 * caller to reject or skip.
 */
export const membersOf = (
  ast: SchemaAST.AST
): ReadonlyArray<
  { readonly ast: SchemaAST.AST; readonly struct: SchemaAST.Objects | undefined; readonly tag: string | undefined }
> => {
  if (ast._tag === "Union") return ast.types.flatMap(membersOf)
  const struct = structOf(ast)
  return [{ ast, struct, tag: struct === undefined ? undefined : tagOf(struct) }]
}
