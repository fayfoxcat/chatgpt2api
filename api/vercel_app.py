"""
Vercel Serverless Function entry point for the FastAPI backend.
This file is used when deploying to Vercel.
"""
from api.app import create_app

app = create_app()
