"""Which attributes actually populate a retriever span's input and output?

A retriever span can be described in more than one attribute convention, and the
conventions are not interchangeable: which ones are read depends on whether the
span also declares an OpenInference span kind. This harness sends the same
retrieval, once per convention, and shows what the deployment stored for each.

Three outcomes are possible, and the third one is easy to miss. A convention can
be read as intended, or ignored so the span lands with empty content, or rejected
at validation. Rejection discards every span in the same export request, the
healthy parent span included, and the HTTP response is still a success: the only
signal is a partialSuccess block in the response body, which most exporters drop
on the floor. Variants marked "rejected" below assert exactly that.

    inspect mode (default)  prints each variant's attributes. No network.
    live mode               posts them and reads back the stored span input,
                            output and span type, then checks expectations.

Every result here was measured through these endpoints, and nothing else:

    POST /otel/v1/traces                   ingest, one export request per variant
    POST /projects/paginated               resolve the project by name
    GET  /projects/{id}/log_streams        resolve the variant's log stream
    POST /projects/{id}/spans/search       read the stored span back

Note what that does and does not cover. Ingest is exercised by posting OTLP
directly over HTTP, so the attribute values are the ones that arrive at the
endpoint. An application reaching the same endpoint through an OTel SDK exporter,
or through a gateway of its own, has upstream steps that this harness does not
see and cannot vouch for.

Usage:
    python3 retriever_span_attributes.py
    python3 retriever_span_attributes.py --mode live
    python3 retriever_span_attributes.py --mode live --only v1 v6 --brief

No third-party packages are needed. Python 3.9 or newer.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any

INGEST_PATH = "/otel/v1/traces"
READBACK_TIMEOUT_S = 90
# A rejected export request never lands, so proving absence only needs long enough
# for a healthy request posted at the same moment to have shown up.
REJECT_SETTLE_S = 20

QUERY = "How do I reset my password?"

DOCUMENTS = [
    {"id": "doc1", "content": "A password reset can be started from the account settings page"},
    {"id": "doc2", "content": "The reset link expires after fifteen minutes and can be requested again"},
]

# A stored document keeps only content, page_content and metadata, so an application's
# own id is dropped from the top level and has to travel inside metadata to survive.
DOCUMENTS_WITH_METADATA = [{"content": d["content"], "metadata": {"id": d["id"]}} for d in DOCUMENTS]

# Only scalar metadata values are kept. This set mixes all four accepted types with one
# nested value, so a single run shows which keys come back and which vanish.
DOCUMENTS_WITH_RICH_METADATA = [
    {
        "content": d["content"],
        "metadata": {
            "id": d["id"],
            "relevance_score": score,
            "source": f"kb://support/passwords/{d['id']}",
            "chunk_index": index,
            "reranked": True,
            "provenance": {"index": "support-kb", "revision": 4},
        },
    }
    for index, (d, score) in enumerate(zip(DOCUMENTS, (0.93, 0.71)))
]

# Each variant differs only in the attributes on the retriever span. The parent
# agent span is identical everywhere so the retrieval always sits in the same
# place in the tree.
#
#   expect_input     substring required in the stored span input, or None for blank
#   expect_output    substring required in the stored span output, or None for blank
#   expect_output_absent  optional substring that must NOT appear in the stored output
#   expect_rejected  set instead of the two above when the whole export request is
#                    expected to be refused at validation, so nothing lands at all
VARIANTS: dict[str, dict[str, Any]] = {
    "v1": {
        "summary": "as reported: OpenInference kind, with the query and documents on the gen_ai.* attributes",
        "attributes": {
            "db.operation": "query",
            "openinference.span.kind": "retriever",
            "gen_ai.input.messages": QUERY,
            "gen_ai.output.messages": json.dumps(DOCUMENTS),
        },
        "expect_input": None,
        "expect_output": None,
        "why": (
            "the query is not a message list, and the OpenInference retriever path rebuilds the "
            "output from its own attributes, which is also what keeps this one from being rejected"
        ),
    },
    "v2": {
        "summary": "OpenInference kind, query and documents on input.value and output.value",
        "attributes": {
            "db.operation": "query",
            "openinference.span.kind": "retriever",
            "input.value": QUERY,
            "output.value": json.dumps(DOCUMENTS),
        },
        "expect_input": QUERY,
        "expect_output": "fifteen minutes",
        "why": (
            "the OpenInference path reads input.value and output.value; note that a document keeps "
            "only content, page_content and metadata, so any other key is dropped"
        ),
    },
    "v3": {
        "summary": "OpenInference kind, output.value wrapping the list under a documents key",
        "attributes": {
            "db.operation": "query",
            "openinference.span.kind": "retriever",
            "input.value": QUERY,
            "output.value": json.dumps({"documents": DOCUMENTS}),
        },
        "expect_input": QUERY,
        "expect_output": "fifteen minutes",
        "why": "a documents key is unwrapped, so both output.value shapes are accepted",
    },
    "v4": {
        "summary": "OpenInference kind, documents as a JSON string on retrieval.documents",
        "attributes": {
            "db.operation": "query",
            "openinference.span.kind": "retriever",
            "input.value": QUERY,
            "retrieval.documents": json.dumps(DOCUMENTS),
        },
        "expect_input": QUERY,
        "expect_output": None,
        "why": "retrieval.documents is only read when it arrives as a list, and a JSON string is not one",
    },
    "v5": {
        "summary": "OpenInference kind, documents as an array attribute of strings on retrieval.documents",
        "attributes": {
            "db.operation": "query",
            "openinference.span.kind": "retriever",
            "input.value": QUERY,
        },
        "array_attributes": {"retrieval.documents": [d["content"] for d in DOCUMENTS]},
        "expect_rejected": True,
        "why": (
            "an OTLP array attribute does arrive as a list, but it can only carry plain strings, "
            "and retriever output has to be a list of document objects"
        ),
    },
    "v6": {
        "summary": "no OpenInference kind, message lists on the gen_ai.* attributes, documents as the content",
        "attributes": {
            "db.operation": "query",
            "gen_ai.input.messages": json.dumps([{"role": "user", "content": QUERY}]),
            "gen_ai.output.messages": json.dumps([{"role": "assistant", "content": DOCUMENTS}]),
        },
        "expect_input": QUERY,
        "expect_output": "fifteen minutes",
        "why": "without an OpenInference kind the gen_ai.* attributes are read as sent",
    },
    "v7": {
        "summary": "as v6 but typed explicitly with gen_ai.operation.name instead of db.operation",
        "attributes": {
            "gen_ai.operation.name": "retriever",
            "gen_ai.input.messages": json.dumps([{"role": "user", "content": QUERY}]),
            "gen_ai.output.messages": json.dumps([{"role": "assistant", "content": DOCUMENTS}]),
        },
        "expect_input": QUERY,
        "expect_output": "fifteen minutes",
        "why": "the operation name types the span, so no database attribute is needed",
    },
    "v8": {
        "summary": "no OpenInference kind, but the query and documents sent the way v1 sends them",
        "attributes": {
            "db.operation": "query",
            "gen_ai.input.messages": QUERY,
            "gen_ai.output.messages": json.dumps(DOCUMENTS),
        },
        "expect_rejected": True,
        "why": (
            "isolates the OpenInference kind: the same value shapes are not merely ignored without "
            "it, they cost the whole export request"
        ),
    },
    "v9": {
        "summary": "as v2 but each document carries its id inside metadata",
        "attributes": {
            "db.operation": "query",
            "openinference.span.kind": "retriever",
            "input.value": QUERY,
            "output.value": json.dumps(DOCUMENTS_WITH_METADATA),
        },
        "expect_input": QUERY,
        "expect_output": "doc1",
        "why": "metadata is one of the three fields a document keeps, so it is where an id survives",
    },
    "v10": {
        "summary": "as v6 but each document carries its id inside metadata",
        "attributes": {
            "db.operation": "query",
            "gen_ai.input.messages": json.dumps([{"role": "user", "content": QUERY}]),
            "gen_ai.output.messages": json.dumps(
                [{"role": "assistant", "content": DOCUMENTS_WITH_METADATA}]
            ),
        },
        "expect_input": QUERY,
        "expect_output": "doc1",
        "why": "confirms the metadata placement survives on this convention too, not only on v9's",
    },
    "v11": {
        "summary": "as v10 but metadata carries a relevance score, more scalars, and one nested value",
        "attributes": {
            "db.operation": "query",
            "gen_ai.input.messages": json.dumps([{"role": "user", "content": QUERY}]),
            "gen_ai.output.messages": json.dumps(
                [{"role": "assistant", "content": DOCUMENTS_WITH_RICH_METADATA}]
            ),
        },
        "expect_input": QUERY,
        "expect_output": "0.93",
        "expect_output_absent": "support-kb",
        "why": (
            "a score has no field of its own, so metadata is the only place for it; the nested "
            "provenance value measures what happens to a metadata value that is not a scalar"
        ),
    },
}

RETRIEVER_SPAN_NAME = "query get_support_docs"
AGENT_SPAN_NAME = "support assistant turn"


# --- formatting -------------------------------------------------------------


def rule(title: str = "") -> None:
    print("\n" + (f"== {title} " + "=" * max(0, 76 - len(title)) if title else "=" * 78))


def outcome(spec: dict) -> str:
    """The one-phrase verdict for a variant, for the summary table."""
    if spec.get("expect_rejected"):
        return "request rejected"
    if spec["expect_output"] is not None:
        return "documents stored"
    return "input only" if spec["expect_input"] is not None else "nothing stored"


def clip(value: Any, width: int = 96) -> str:
    if value is None:
        return "<none>"
    text = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"))
    if text == "":
        return "<empty>"
    text = text.replace("\n", "\\n")
    return text if len(text) <= width else text[: width - 3] + "..."


# --- payload construction ---------------------------------------------------


def attribute(key: str, value: Any) -> dict:
    return {"key": key, "value": {"stringValue": value}}


def array_attribute(key: str, values: list[str]) -> dict:
    return {"key": key, "value": {"arrayValue": {"values": [{"stringValue": v} for v in values]}}}


def export_request(variant: str, run_tag: str) -> dict:
    """Build one OTLP export request: an agent parent span with a retriever child."""
    spec = VARIANTS[variant]

    def span_id(role: str, width: int) -> str:
        return hashlib.md5(f"{run_tag}-{variant}-{role}".encode()).hexdigest()[:width]

    trace_id = span_id("trace", 32)
    agent_span_id = span_id("agent", 16)
    retriever_span_id = span_id("retriever", 16)
    # Two spans sharing an id means one silently replaces the other, which reads as
    # an ingestion problem rather than a payload problem. Fail here instead.
    assert agent_span_id != retriever_span_id

    now_ns = int(time.time() * 1_000_000_000)
    agent_attributes = [
        attribute("gen_ai.operation.name", "invoke_agent"),
        attribute("gen_ai.agent.name", "support-assistant"),
        attribute("gen_ai.input.messages", json.dumps([{"role": "user", "content": QUERY}])),
        attribute(
            "gen_ai.output.messages",
            json.dumps([{"role": "assistant", "content": "Start the reset from account settings. The link lasts fifteen minutes."}]),
        ),
    ]

    retriever_attributes = [attribute(k, v) for k, v in spec["attributes"].items()]
    for key, values in (spec.get("array_attributes") or {}).items():
        retriever_attributes.append(array_attribute(key, values))

    return {
        "resourceSpans": [
            {
                "resource": {"attributes": [attribute("service.name", "retriever-span-attributes")]},
                "scopeSpans": [
                    {
                        "scope": {"name": "retriever-span-attributes"},
                        "spans": [
                            {
                                "traceId": trace_id,
                                "spanId": agent_span_id,
                                "name": AGENT_SPAN_NAME,
                                "kind": 1,
                                "startTimeUnixNano": str(now_ns),
                                "endTimeUnixNano": str(now_ns + 900_000_000),
                                "attributes": agent_attributes,
                                "status": {},
                            },
                            {
                                "traceId": trace_id,
                                "spanId": retriever_span_id,
                                "parentSpanId": agent_span_id,
                                "name": RETRIEVER_SPAN_NAME,
                                "kind": 1,
                                "startTimeUnixNano": str(now_ns + 100_000_000),
                                "endTimeUnixNano": str(now_ns + 300_000_000),
                                "attributes": retriever_attributes,
                                "status": {},
                            },
                        ],
                    }
                ],
            }
        ]
    }


# --- API client -------------------------------------------------------------


class Galileo:
    def __init__(self, console_url: str, api_key: str) -> None:
        host = console_url.rstrip("/")
        # Local dev serves the console on one port and the API on another.
        self.base = "http://localhost:8088" if "localhost" in host or "127.0.0.1" in host else host
        self.key = api_key

    def _call(self, path: str, body: Any = None, method: str = "POST", extra: dict | None = None) -> Any:
        headers = {"Galileo-API-Key": self.key, "Content-Type": "application/json"}
        headers.update(extra or {})
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read() or b"{}")
        except urllib.error.HTTPError as e:
            sys.exit(f"\n{method} {path} failed with HTTP {e.code}:\n  {e.read().decode()[:500]}")

    def ingest(self, payload: dict, project: str, log_stream: str) -> dict:
        """Post one export request and return whatever it refused, which may be everything."""
        reply = self._call(INGEST_PATH, payload, extra={"project": project, "logstream": log_stream})
        return (reply or {}).get("partialSuccess") or {}

    def project_id(self, name: str) -> str:
        found = self._call("/projects/paginated", {"filters": [{"name": "name", "operator": "eq", "value": name}]})
        projects = found.get("projects", [])
        if not projects:
            sys.exit(f"project {name!r} not found after ingest")
        return projects[0]["id"]

    def log_stream_id(self, project_id: str, name: str) -> str | None:
        for ls in self._call(f"/projects/{project_id}/log_streams", method="GET"):
            if ls.get("name") == name:
                return ls["id"]
        return None

    def spans(self, project_id: str, log_stream_id: str) -> list[dict]:
        found = self._call(f"/projects/{project_id}/spans/search", {"log_stream_id": log_stream_id, "limit": 100})
        return found.get("records", [])


def wait_for_span(api: Galileo, project: str, log_stream: str, span_name: str) -> dict | None:
    """Ingestion is asynchronous, so poll until the retriever span has landed."""
    project_id = api.project_id(project)
    deadline = time.time() + READBACK_TIMEOUT_S
    while time.time() < deadline:
        log_stream_id = api.log_stream_id(project_id, log_stream)
        if log_stream_id:
            for record in api.spans(project_id, log_stream_id):
                if record.get("name") == span_name:
                    return record
        time.sleep(2)
    return None


def stored_span_names(api: Galileo, project: str, log_stream: str) -> list[str]:
    """Every span name the log stream holds, once the stream has had time to fill."""
    time.sleep(REJECT_SETTLE_S)
    project_id = api.project_id(project)
    log_stream_id = api.log_stream_id(project_id, log_stream)
    if log_stream_id is None:
        return []
    return [record.get("name", "") for record in api.spans(project_id, log_stream_id)]


# --- modes ------------------------------------------------------------------


def inspect(variant: str, brief: bool) -> None:
    spec = VARIANTS[variant]
    rule(f"{variant}: {spec['summary']}")
    if spec.get("expect_rejected"):
        print("  expected result:  the whole export request is rejected, so nothing lands")
    else:
        print(f"  expected input :  {clip(spec['expect_input'])}")
        print(f"  expected output:  {'<blank>' if spec['expect_output'] is None else 'the documents'}")
    print(f"  because:          {spec['why']}")
    print("\n  attributes on the retriever span:")
    for key, value in spec["attributes"].items():
        print(f"    {key} = {clip(value, 70)}")
    for key, values in (spec.get("array_attributes") or {}).items():
        print(f"    {key} = array of {len(values)} strings")

    if not brief:
        print("\n  full export request:")
        print(json.dumps(export_request(variant, "inspect"), indent=2))


def live(variant: str, api: Galileo, project: str, log_stream_base: str, run_tag: str, brief: bool) -> bool:
    spec = VARIANTS[variant]
    log_stream = f"{log_stream_base}-{variant}-{run_tag}"

    rule(f"{variant}: {spec['summary']}")
    print(f"  log stream:  {log_stream}")

    refused = api.ingest(export_request(variant, run_tag), project, log_stream)
    rejected = int(refused.get("rejectedSpans") or 0)
    reason = (refused.get("errorMessage") or "").strip()
    if refused:
        print("\n  the request was refused, and the HTTP status was a success anyway:")
        print(f"    spans rejected:  {rejected} of the 2 sent")
        print(f"    reason given  :  {clip(reason, 180)}")

    if spec.get("expect_rejected"):
        landed = stored_span_names(api, project, log_stream)
        print(f"\n  spans the log stream holds after {REJECT_SETTLE_S}s:  {landed or '<none>'}")
        checks = [
            ("the export request was refused", rejected > 0),
            ("the refusal names the offending field", "retriever" in reason.lower()),
            ("the retriever span did not land", RETRIEVER_SPAN_NAME not in landed),
            ("the healthy parent span did not land either", AGENT_SPAN_NAME not in landed),
        ]
        print()
        for label, ok in checks:
            print(f"    {'ok  ' if ok else 'FAIL'}  {label}")
        return all(ok for _, ok in checks)

    record = wait_for_span(api, project, log_stream, RETRIEVER_SPAN_NAME)
    if record is None:
        print(f"\n  FAIL: the retriever span never appeared within {READBACK_TIMEOUT_S}s")
        return False

    stored_input = record.get("input")
    stored_output = record.get("output")
    span_type = record.get("type")

    print("\n  what was stored on the retriever span:")
    print(f"    type   :  {span_type}")
    print(f"    input  :  {clip(stored_input)}")
    print(f"    output :  {clip(stored_output)}")

    if not brief:
        print("\n  the stored span, as the read API returns it:")
        print(json.dumps({k: record.get(k) for k in ("name", "type", "input", "output")}, indent=2)[:1500])

    input_text = stored_input if isinstance(stored_input, str) else json.dumps(stored_input or "")
    output_text = stored_output if isinstance(stored_output, str) else json.dumps(stored_output or "")

    checks: list[tuple[str, bool]] = [
        ("nothing in the export request was refused", not refused),
        ("span is typed as a retriever", span_type == "retriever"),
    ]
    if spec["expect_input"] is None:
        checks.append(("input is blank, as expected", input_text.strip() in ("", '""')))
    else:
        checks.append((f"input carries the query", spec["expect_input"] in input_text))
    if spec["expect_output"] is None:
        checks.append(("output is blank, as expected", output_text.strip() in ("", '""', "[]", '"[]"')))
    else:
        checks.append((f"output carries the documents", spec["expect_output"] in output_text))
    if spec.get("expect_output_absent") is not None:
        absent = spec["expect_output_absent"]
        checks.append((f"output does not carry {absent!r}", absent not in output_text))

    print()
    for label, ok in checks:
        print(f"    {'ok  ' if ok else 'FAIL'}  {label}")
    return all(ok for _, ok in checks)


# --- entry point ------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=("inspect", "live"), default="inspect")
    parser.add_argument("--only", nargs="+", choices=list(VARIANTS), metavar="VARIANT")
    parser.add_argument("--brief", action="store_true", help="omit the full payload and record dumps")
    args = parser.parse_args()

    variants = args.only or list(VARIANTS)

    if args.mode == "inspect":
        for variant in variants:
            inspect(variant, args.brief)
        print("\n  inspect mode only describes what is sent. Run --mode live to see what a")
        print("  deployment actually stores for each variant.")
        return 0

    console_url = os.environ.get("GALILEO_CONSOLE_URL")
    api_key = os.environ.get("GALILEO_API_KEY")
    project = os.environ.get("GALILEO_PROJECT")
    log_stream = os.environ.get("GALILEO_LOG_STREAM")
    if not all((console_url, api_key, project, log_stream)):
        sys.exit(
            "live mode needs GALILEO_CONSOLE_URL, GALILEO_API_KEY, GALILEO_PROJECT and "
            "GALILEO_LOG_STREAM set.\nSee .env.sample."
        )

    api = Galileo(console_url, api_key)
    run_tag = format(int(time.time()) % 100_000_000, "08x")
    results = {v: live(v, api, project, log_stream, run_tag, args.brief) for v in variants}

    rule("SUMMARY")
    for variant, ok in results.items():
        spec = VARIANTS[variant]
        print(f"  {variant}  {'PASS' if ok else 'FAIL'}  {outcome(spec):<17}  {spec['summary']}")
    print(f"\n  project {project}, log streams suffixed -{run_tag}")
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
