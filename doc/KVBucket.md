# Revisioned blob bucket
- guid as range index
- listing using distinct to filter out revisions
- need static 'latest revision' property for CAS
    - not a feature in DynamoDB

## API

### `/{name}`
- `GET`: Redirect to `/{name}/`

### `/{name}/`
- `GET`: List of all properties defined on the object.
- `POST`: Potentially an alternative for form-based creation of new
  properties.

### `/{name}/{prop}`
- `GET`: Latest revision of a page property.
- `PUT`: Save a new revision of an object property. The `tid` for the new
  property revision is returned.
- `POST`: Post a HTTP transaction with potentially several sub-requests to
  atomically create a new object revision. The primary transaction member is
  normally the one posted to.

### `/{name}/{prop}/`
- `GET`: List revisions (by `tid`) of the given property.

### `/{name}/{prop}/{rev}`
- `GET`: Retrieve a property at a given revision. 
- `PUT`: Create a property with the given revision. Requires elevated rights.

### Format
`Revision` can be one of:
- `UUID`: A specific UUID-based revision
- `date` in the past: The revision that was active at a specific time.

`Property` is a string. Examples: `html`, `wikitext`, `data-parsoid`.

