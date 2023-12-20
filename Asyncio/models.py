
from sqlalchemy import JSON, Column, Integer

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

from sqlalchemy.orm import sessionmaker


import os


PG_USER=os.getenv('PG_USER', 'postgres')

PG_PASSWORD=os.getenv('PG_PASSWORD', '1234')

PG_DB=os.getenv('PG_DB', 'netology')

PG_HOST=os.getenv('PG_HOST', '127.0.0.1')

PG_PORT=os.getenv('PG_PORT', '5431')


PG_DSN=f"postgresql+asyncpg://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"



engine = create_async_engine(PG_DSN)

Session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)



Base=declarative_base()


class SwapiPeople(Base):

    __tablename__ = 'swapi_people'
    id = Column(Integer, primary_key=True)
    json=Column(JSON)


