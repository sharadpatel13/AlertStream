import pandas as pd
import numpy as np
import faiss
import os
from sentence_transformers import SentenceTransformer

DATA_PATH = "data/historical_incidents.csv"
VECTOR_PATH = "intelligence/vectors"
MODEL_NAME = "all-MiniLM-L6-v2"

os.makedirs(VECTOR_PATH, exist_ok=True)

print("Loading historical incidents...")
df = pd.read_csv(DATA_PATH)

# Combine meaningful text fields
df["text"] = (
    df["system"] + " " +
    df["component"] + " " +
    df["alert_type"] + " " +
    df["description"]
)

texts = df["text"].tolist()

print("Loading embedding model...")
model = SentenceTransformer(MODEL_NAME)

print("Generating embeddings...")
embeddings = model.encode(texts)

dimension = embeddings.shape[1]

# Build FAISS index
index = faiss.IndexFlatL2(dimension)
index.add(np.array(embeddings).astype("float32"))

# Save everything
faiss.write_index(index, f"{VECTOR_PATH}/incident_index.faiss")
np.save(f"{VECTOR_PATH}/incident_vectors.npy", embeddings)
df.to_pickle(f"{VECTOR_PATH}/incident_metadata.pkl")

print("Embedding index created successfully.")
print(f"Total incidents indexed: {len(df)}")
