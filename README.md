# Which attributes populate a retriever span's input and output?

A retrieval span can be described in more than one attribute convention, and the conventions are not
interchangeable. This repo settles which ones a deployment actually reads, by sending the same
retrieval ten different ways over OpenTelemetry and reading back what was stored for each.

It exists because the documented minimum for a retriever span can be satisfied in a way that still
produces an empty span, with nothing in the response to say so.

## The two conventions, and why mixing them is the trap

A span can carry retrieval content on the OpenTelemetry GenAI attributes (`gen_ai.input.messages`
and `gen_ai.output.messages`) or on the OpenInference attributes (`input.value`, `output.value`,
`retrieval.documents`).

Declaring `openinference.span.kind` is what decides between them. It is not a label: it hands the
span to a different reader. On a span whose kind is `retriever`, that reader rebuilds
`gen_ai.output.messages` from its own attributes before anything else looks at it, so a value set
there by hand is replaced rather than merged. Setting the OpenInference kind and then describing the
documents on the GenAI attributes therefore describes them somewhere that is about to be overwritten.

Two further constraints apply whichever convention is used:

- The `gen_ai.*.messages` attributes are message lists, not free text. The value has to be a
  JSON-encoded array of objects, each with a `role` and a `content`. A bare query string is not a
  message list, and an object with no `role` is skipped, so a list of plain document objects reduces
  to nothing.
- A retriever span's output has to be a list of document objects. Document text goes in `content`
  (`page_content` is accepted as an alias), and anything other than those and `metadata` is dropped,
  so an application's own document id has to travel in `metadata` to survive. Inside `metadata`, only
  scalar values are kept: strings, numbers and booleans survive, and anything nested is discarded
  without an error. So a relevance score is fine as a number, but a nested object is not.

## Three outcomes, and the third one is quiet

A convention can be read as intended, or ignored so the span lands with empty content, or **rejected
at validation**. Rejection discards every span in the same export request, including healthy parent
spans that had nothing wrong with them, and the HTTP response is still a success. The only signal is
a `partialSuccess` block in the response body naming the offending field, and most exporters discard
that body without looking at it.

So a malformed retrieval span does not cost you the retrieval span. It can cost you the whole turn.

## What was measured

Each variant sends one export request: an agent parent span representing the turn, with a retrieval
span nested under it. Only the retrieval span's attributes differ.

| variant | how the retrieval is described | stored input | stored output |
|---------|-------------------------------|--------------|---------------|
| v1 | OpenInference kind, query and documents on the `gen_ai.*` attributes | empty | empty list |
| v2 | OpenInference kind, `input.value` and `output.value` as a JSON array | the query | the documents |
| v3 | OpenInference kind, `output.value` as an object with a `documents` key | the query | the documents |
| v4 | OpenInference kind, `retrieval.documents` as a JSON string | the query | empty list |
| v5 | OpenInference kind, `retrieval.documents` as an array of plain strings | request rejected | request rejected |
| v6 | no OpenInference kind, `gen_ai.*` message lists with the documents as the content | the query | the documents |
| v7 | as v6, typed with `gen_ai.operation.name` instead of `db.operation` | the query | the documents |
| v8 | no OpenInference kind, values shaped as in v1 | request rejected | request rejected |
| v9 | as v2, with each document's id inside `metadata` | the query | the documents, ids intact |
| v10 | as v6, with each document's id inside `metadata` | the query | the documents, ids intact |

A rendered run is committed under `evidence/`, so the results can be read without a deployment to
hand.

### What that leaves you

Two shapes work, and either is fine to standardise on:

- **Stay on OpenInference** (v2, v3 or v9). Put the query in `input.value` and the documents in
  `output.value` as a JSON array, or as an object with a `documents` key. `retrieval.documents` is
  read only when it arrives as a genuine list, and an OpenTelemetry array attribute can hold only
  plain strings, never objects, which is why v4 lands empty and v5 is refused outright.
- **Drop the OpenInference kind** (v6, v7 or v10). Send `gen_ai.input.messages` as a one-message array
  and `gen_ai.output.messages` as a one-message array whose `content` is the array of documents.
  Nothing rewrites those attributes once the OpenInference kind is absent.

The `metadata` placement for document ids was measured on both conventions, v9 and v10, so it is not
specific to either.

v1 is the shape that reads most naturally from the documented minimum, and it is the one that stores
nothing. v8 is v1 with the OpenInference kind removed, and it shows that the kind was the only thing
keeping that request from being rejected.

### Typing the span

Two ways to have the span recognised as a retrieval, both verified: `db.operation` set to `query` or
`search`, or `gen_ai.operation.name` set to `retriever`. Neither affects content extraction, so a
span can be correctly typed as a retrieval, with the blue document icon, and still be empty. The icon
confirms typing worked, nothing more.

## Requirements

Python 3.9 or newer, standard library only. There is nothing to install and no SDK version to pin.

Live mode needs a deployment URL and an API key. Copy `.env.sample` to `.env`, fill it in, then:

```
set -a; source .env; set +a
```

## Running it

```
python3 retriever_span_attributes.py                             # inspect, no network, no credentials
python3 retriever_span_attributes.py --mode live                 # post, read back, check
python3 retriever_span_attributes.py --mode live --only v1 v6     # two variants
python3 retriever_span_attributes.py --mode live --brief          # skip the payload and record dumps
```

Inspect mode only describes what is sent, and makes no claim about the result. Live mode is where the
behaviour is tested: it posts to `/otel/v1/traces`, reads the stored spans back through
`/projects/{id}/spans/search`, prints a check grid per variant and exits non-zero if any expectation
is not met. Each variant gets its own log stream, suffixed with a per-run tag, so runs never mix.
