Azolla defines the retry policy of task with a **language‑agnostic, JSON‑serialisable specification**. All fields are optional — unspecified fields take the defaults shown.

---

## 1. Top‑level shape

```jsonc
{
  "version": 1,          // reserved for future breaking changes
  "stop":  { ... },      // when to give up
  "wait":  { ... },      // how long to wait before the *next* attempt
  "retry": { ... }       // which exceptions trigger a retry
}
```

If the policy object is omitted entirely, the scheduler behaves as if the defaults (below) were supplied.

---

## 2. `stop` – **when to stop retrying**

| field          | type                  | default | meaning                                                                          |
| -------------- | --------------------- | ------- | -------------------------------------------------------------------------------- |
| `max_attempts` | integer ≥ 1 \| `null` | **5**   | maximum *total* attempts (first run + retries). `null` = no attempt cap          |
| `max_delay`    | number ≥ 0 \| `null`  | `null`  | wall‑clock seconds since *task creation* after which the scheduler must give up. |

**Semantics**

* A stop condition fires when **either** limit is reached.
  (So `{"max_attempts": 5, "max_delay": 3600}` stops after *5 attempts* **or** *1 h* of elapsed time, whichever happens first.)
* If both fields are `null`, the task may retry forever.

---

## 3. `wait` – **how to compute the delay for the next attempt**

| field           | type / allowed values                                | default                    | notes                                           |
| --------------- | ---------------------------------------------------- | -------------------------- | ----------------------------------------------- |
| `strategy`      | `"fixed"` / `"exponential"` / `"exponential_jitter"` | **`"exponential_jitter"`** |                                                 |
| `delay`         | number ≥ 0                                           | **1**                      | used only when `strategy = "fixed"`             |
| `initial_delay` | number ≥ 0                                           | **1**                      | delay after the *first* failure (seconds)       |
| `multiplier`    | number ≥ 1                                           | **2**                      | growth factor for successive attempts           |
| `max_delay`     | number ≥ 0                                           | **300**                    | hard cap on the delay returned by the algorithm |

**Algorithms**

* **`fixed`** `next_delay = delay`
* **`exponential`** `next_delay = min(initial_delay × multiplier^(attempt‑1), max_delay)`
* **`exponential_jitter`** compute the exponential delay above, then pick a uniform random value in `[0, computed_delay]` (always with full jitter)

Attempt numbers start at 0 for the *first attempt* (i.e. the original run). The first retry is attempt 1.

---

## 4. `retry` – **which failures are retryable**

| field                | type                        | default                  | meaning                                                                                            |
| -------------------- | --------------------------- | ------------------------ | -------------------------------------------------------------------------------------------------- |
| `include_errors` | array of string-ErrorResult‑types | `["ValueError"]` | *Any* listed triggers a retry. Empty array ⇒ “retry on **nothing**”.         |
| `exclude_errors` | array of string‑ErrorResult‑types | `[]`                     | If an error matches **any** here, do **not** retry, even if it’s also in `include_errors`. |

---

## 5. Defaults (safety‑first)

If a field or section is absent, apply the values in **bold** above:

```json
{
  "version": 1,
  "stop":  { "max_attempts": 5 },
  "wait":  {
    "strategy": "exponential_jitter",
    "initial_delay": 1,
    "multiplier": 2,
    "max_delay": 300
  },
  "retry": { "include_errors": ["ValueError"] }
}
```

These defaults give you **five attempts**, exponential back‑off capped at **5 min**, full jitter, and “retry on any exception” — addressing the unsafe defaults noted earlier.

---

## 6. Example policies

### a) Fixed 10‑second delay, retry network errors only, for up to 30 min

```json
{
  "version": 1,
  "stop":  { "max_delay": 1800 },
  "wait":  { "strategy": "fixed", "delay": 10 },
  "retry": { "include_errors": [
               "ConnectionError",
               "TimeoutError",
               "NetworkError"
             ] }
}
```

### b) Infinite retries with cubic back‑off (multiplier = 3) but no jitter

```json
{
  "version": 1,
  "stop":  { "max_attempts": null, "max_delay": null },
  "wait":  {
    "strategy": "exponential",
    "initial_delay": 2,
    "multiplier": 3,
    "max_delay": 600
  }
}
```

---

## 7. Notes for implementers

* **Forward compatibility** – unknown top‑level keys **must be ignored**, but unknown keys inside `stop`, `wait`, or `retry` **should raise an error**, so that misspellings don’t go unnoticed.
