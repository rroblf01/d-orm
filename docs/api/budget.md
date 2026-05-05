# `dorm.budget`

Per-block query budget — wall-clock timeout + max-rows ceiling.
Both context-manager forms, sync and async; `BudgetExceeded`
subclasses `DatabaseError`.

See [framework-agnostic helpers](../helpers.md#query-budget) for
recipes.

## API

::: dorm.budget.budget
::: dorm.budget.abudget
::: dorm.budget.BudgetExceeded
::: dorm.budget.current
::: dorm.budget.check_rowcount
