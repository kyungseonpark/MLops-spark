from pydantic import BaseModel

class PredictionItem(BaseModel):
    data: dict
