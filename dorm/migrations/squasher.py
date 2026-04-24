from __future__ import annotations

from .operations import AddField, AlterField, CreateModel, DeleteModel, RemoveField


def squash_operations(operations: list) -> list:
    """Single-pass optimizer for a list of migration operations.

    Merges / eliminates redundant pairs so the squashed migration is as
    compact as possible.
    """
    changed = True
    ops = list(operations)

    while changed:
        changed = False
        result: list = []
        skip: set[int] = set()

        for i, op in enumerate(ops):
            if i in skip:
                continue

            merged = False
            for j in range(i + 1, len(ops)):
                if j in skip:
                    continue
                next_op = ops[j]

                # CreateModel(X) + DeleteModel(X) → remove both plus all X ops between
                if (
                    isinstance(op, CreateModel)
                    and isinstance(next_op, DeleteModel)
                    and op.name.lower() == next_op.name.lower()
                ):
                    model = op.name.lower()
                    skip.add(j)
                    for k in range(i + 1, j):
                        if k in skip:
                            continue
                        mid = ops[k]
                        mn = getattr(mid, "model_name", None)
                        if mn is not None and mn.lower() == model:
                            skip.add(k)
                    skip.add(i)
                    changed = True
                    merged = True
                    break

                # CreateModel(X) + AddField(X, f) → merge field into CreateModel
                if (
                    isinstance(op, CreateModel)
                    and isinstance(next_op, AddField)
                    and op.name.lower() == next_op.model_name.lower()
                ):
                    new_fields = list(op.fields) + [(next_op.name, next_op.field)]
                    new_op = CreateModel(op.name, new_fields, op.options)
                    result.append(new_op)
                    skip.add(j)
                    changed = True
                    merged = True
                    break

                # AddField(X, f) + RemoveField(X, f) → eliminate both
                if (
                    isinstance(op, AddField)
                    and isinstance(next_op, RemoveField)
                    and op.model_name.lower() == next_op.model_name.lower()
                    and op.name == next_op.name
                ):
                    skip.add(j)
                    skip.add(i)
                    changed = True
                    merged = True
                    break

                # AddField(X, f) + AlterField(X, f, new_type) → AddField(X, f, new_type)
                if (
                    isinstance(op, AddField)
                    and isinstance(next_op, AlterField)
                    and op.model_name.lower() == next_op.model_name.lower()
                    and op.name == next_op.name
                ):
                    new_op = AddField(op.model_name, op.name, next_op.field)
                    result.append(new_op)
                    skip.add(j)
                    changed = True
                    merged = True
                    break

            if not merged and i not in skip:
                result.append(op)

        ops = result

    return ops
