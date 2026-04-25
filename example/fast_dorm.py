"""FastAPI example using dorm + DormSchema.

DormSchema (Django-REST-style):
  - Inner ``class Meta`` chooses the dorm Model and the field set.
  - Anything you declare in the class body — extra fields, type
    overrides, ``@field_validator`` decorators — wins over the
    Meta-derived defaults.
  - ``from_attributes=True`` is on by default, so FastAPI can serialize
    a dorm instance directly via ``response_model=YourSchema``.
"""

from fastapi import FastAPI
from pydantic import field_validator

from dorm.contrib.pydantic import DormSchema

from .sales.models import Customer

app = FastAPI()


# ── Schemas ───────────────────────────────────────────────────────────────────


class CustomerOut(DormSchema):
    """Response schema — every Customer column."""

    class Meta:
        model = Customer
        fields = "__all__"


class CustomerCreate(DormSchema):
    """POST body: drop the auto-PK, add an extra confirm field, and
    normalize the email via a validator.

    `email` is also declared explicitly here so static type checkers see
    its type when we reference `payload.email` below — Meta auto-fills
    cover the rest (name, phone) at runtime.
    """

    email: str
    confirm_email: str

    @field_validator("email", "confirm_email")
    @classmethod
    def lowercase(cls, v: str) -> str:
        return v.lower()

    class Meta:
        model = Customer
        exclude = ("id",)


# ── Routes ────────────────────────────────────────────────────────────────────


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.post("/customer/", response_model=CustomerOut)
def create_customer(payload: CustomerCreate):
    if payload.email != payload.confirm_email:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="emails do not match")
    return Customer.objects.create(
        **payload.model_dump(exclude={"confirm_email"}, exclude_none=True)
    )


@app.get("/customer/{customer_id}", response_model=CustomerOut)
def get_customer(customer_id: int):
    return Customer.objects.get(id=customer_id)


@app.get("/customers/", response_model=list[CustomerOut])
def list_customers():
    return list(Customer.objects.all())
