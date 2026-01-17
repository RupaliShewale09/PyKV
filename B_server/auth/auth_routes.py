from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, EmailStr, Field
from .auth_service import register_user, login_user

router = APIRouter(prefix="/auth", tags=["Auth"])

class Register(BaseModel):
    username: str = Field(..., min_length=3)
    email: EmailStr
    password: str = Field(..., min_length=8)

class Login(BaseModel):
    username: str
    password: str

@router.post("/register", status_code=201)
def register(data: Register):
    success, msg = register_user(
        data.username, data.email, data.password
    )

    if not success:
        raise HTTPException(status_code=400, detail=msg)

    return {"message": msg}

@router.post("/login")
def login(data: Login):
    success, msg = login_user(
        data.username, data.password
    )

    if not success:
        raise HTTPException(status_code=401, detail=msg)

    return {"message": msg}
