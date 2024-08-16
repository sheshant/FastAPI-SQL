from datetime import datetime
from typing import List

from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

# Database URL
DATABASE_URL = "postgresql+asyncpg://test_user:rootuser@postgresql-181221-0.cloudclusters.net:19415/test_database"

# Async SQLAlchemy engine, session and base
engine = create_async_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(
    bind=engine,
    expire_on_commit=False,
    class_=AsyncSession
)
Base = declarative_base()

app = FastAPI()


# Pydantic model for request/response body
class ItemCreate(BaseModel):
    user_id: int
    article_id: int
    revision_id: int
    namespace: int
    timestamp: datetime
    md5: str
    reverted: int
    reverted_user_id: int
    reverted_revision_id: int
    delta: int
    cur_size: int


class Item(BaseModel):
    user_id: int
    article_id: int
    revision_id: int
    namespace: int
    timestamp: datetime
    md5: str
    reverted: int
    reverted_user_id: int
    reverted_revision_id: int
    delta: int
    cur_size: int

    class Config:
        orm_mode = True


# SQLAlchemy model for the database
class ItemModel(Base):
    __tablename__ = 'wiki_details'
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    user_id = Column(Integer)
    article_id = Column(Integer)
    revision_id = Column(Integer)
    namespace = Column(Integer)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())
    md5 = Column(String)
    reverted = Column(Integer)
    reverted_user_id = Column(Integer)
    reverted_revision_id = Column(Integer)
    delta = Column(Integer)
    cur_size = Column(Integer)


# Dependency to get async session
async def get_db():
    async with SessionLocal() as session:
        yield session


# Initialize database and create tables
@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@app.post("/items/", response_model=Item)
async def create_item(item: ItemCreate, db: AsyncSession = Depends(get_db)):
    db_item = ItemModel(**item.dict())
    db.add(db_item)
    await db.commit()
    await db.refresh(db_item)
    return db_item


@app.post("/items/bulk/", response_model=List[Item])
async def bulk_create_items(items: List[ItemCreate], db: AsyncSession = Depends(get_db)):
    db_items = [ItemModel(**item.dict()) for item in items]
    db.add_all(db_items)
    await db.commit()
    return []


@app.get("/items/{item_id}", response_model=Item)
async def read_item(item_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ItemModel).filter(ItemModel.id == item_id))
    item = result.scalars().first()
    if item is None:
        raise HTTPException(status_code=404, detail="Item not found")
    return item
