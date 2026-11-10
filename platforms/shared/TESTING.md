# Agent test protocol (v2)

Same questions, same instructions, same data on both platforms; differences in answers should be the platform's.
v1 (first run) is kept for comparison: it showed what each platform needed from tuning.

## 1. Configuration to verify BEFORE the run

| | Databricks Genie Agent | Fabric data agent |
|---|---|---|
| Structured data | 9 `p2p_poc.agent.*` tables + `p2p_poc.silver.telemetry` | 9 `agent_*` Lakehouse tables + KQL `Telemetry`, `TelemetryDedup`, `TelemetryMinute`, `TagDim` |
| Documents | managed volume `p2p_poc.agent.documents` (65 files, Beta) | Azure AI Search index `p2p-documents` (preview); your user has **Search Index Data Reader** |
| NOT attached | `bronze.documents_text` | `RawEnvelope` |
| Instructions | `agent_instructions.md` (v2, with the Documents block) + block 2 of `kql_source_instructions.md` | `agent_instructions.md` (v2); per-source: `agent_table_descriptions.md` (Lakehouse), block 1 of `kql_source_instructions.md` (KQL), AI Search description |
| Examples | Spark SQL pairs from `agent_examples.md` | T-SQL and KQL pairs from `agent_examples.md` |
| Mode / runtime | **Agent mode** for every question | **Standard** runtime, published version |

Smoke test each source in a fresh chat first (one question per source) and open the answer's steps: no errors.

## 2. Conversations

| Conversation | Questions |
|---|---|
| 1 (the investigation) | H1 -> H2 -> H3 -> H4 -> H5 -> H6, in order, one conversation |
| 2 | H7 alone; **run it twice** (fresh conversation each) to check consistency |
| 3-17 | Q01 ... Q15, each in a NEW conversation |

Fabric test pane: use **Clear chat** between conversations; copy the answer first (clearing deletes it).

## 3. Recording

- Copy the question text exactly; no rephrasing. First answer only; no corrections. If the agent asks a
  clarifying question, answer neutrally and note it.
- **Time with a stopwatch on both platforms** (Enter -> answer complete). Do not mix with Fabric's displayed time.
- Paste the full answer text into `answer`. Charts: screenshot `<platform>_<question>.png`; copy the underlying
  result table into the answer, or type the numbers with "[read from chart]".
- Optional: paste the generated SQL/KQL into `human_judgement` or a `query` column.
- Save from Excel as **CSV UTF-8**.
- Platform labels: `databricks-v2`, `fabric-v2`. Separate experiments: `databricks-chat` (Chat mode),
  `fabric-preview` (Preview runtime), `fabric-m365copilot`, `databricks-teams`.

## 4. Scoring

```bash
python platforms/shared/score_agents.py --ground-truth ./ground_truth --template answers.csv
python platforms/shared/score_agents.py --ground-truth ./ground_truth --answers answers.csv
```

Automatic checks verify **facts only**: numbers, and key terms of the expected answer (tolerant of paraphrase and
typography); no credit when the answer says it could not retrieve or access the data. In the v1 review they both
missed real errors and rejected correct answers, so **the human judgement is decisive**:

| Score | Meaning |
|---|---|
| 5 | Correct and complete; grounded in the right tables/documents with citations; adds a genuine insight; respects governance |
| 4 | Correct; minor gap (a missed insight, a missing citation, a small slip that does not change the conclusion) |
| 3 | Partly correct or generic: right principle but not grounded in our data/documents, or misses key specifics |
| 2 | Right conclusion for the wrong reason, or a notable factual/governance error |
| 1 | Wrong, contradicts the governing document, or no usable answer (tool failures) |

Note per answer: which sources it used, whether it cited them, and any error. Ground truth (`ground_truth/`) stays on
your laptop: never load it into either platform.

## 5. What v1 taught us (why v2 exists)

- Fabric's v1 document source returned **403**: the data agent calls Azure AI Search with the user's Entra identity,
  which needs Search Index Data Reader (now in Terraform: `search_reader_principal_ids`).
- Neither agent reliably grounded procedural answers (Q06, Q08, Q11) in the SOPs: v2 adds an identical
  document-grounding instruction.
- Databricks used a text table for documents in v1; v2 uses the managed volume, so both platforms use native
  (preview) document retrieval.
