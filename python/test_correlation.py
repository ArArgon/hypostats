from typing import List
import numpy as np
import bisect
from scipy.stats import chi2_contingency
from typing import Union
import torch
from transformers import AutoModel, AutoTokenizer
from sklearn.decomposition import PCA


DEFAULT_N_BINS = 100
model = AutoModel.from_pretrained("avsolatorio/NoInstruct-small-Embedding-v0")
tokenizer = AutoTokenizer.from_pretrained("avsolatorio/NoInstruct-small-Embedding-v0")


def get_embedding(text: Union[str, list[str]], mode: str = "sentence"):
    model.eval()
    assert mode in ("query", "sentence"), f"mode={mode} was passed but only `query` and `sentence` are the supported modes."

    if isinstance(text, str):
        text = [text]

    inp = tokenizer(text, return_tensors="pt", padding=True, truncation=True)

    with torch.no_grad():
        output = model(**inp)

    # The model is optimized to use the mean pooling for queries,
    # while the sentence / document embedding uses the [CLS] representation.

    if mode == "query":
        vectors = output.last_hidden_state * inp["attention_mask"].unsqueeze(2)
        vectors = vectors.sum(dim=1) / inp["attention_mask"].sum(dim=-1).view(-1, 1)
    else:
        vectors = output.last_hidden_state[:, 0, :]
    vectors = vectors.cpu().numpy()

    pca = PCA(n_components=1)
    embedded = pca.fit_transform(vectors)

    return embedded.flatten()


def test_kl_divergence(col1: List, bins1: List, col2: List, bins2: List) -> float:
    # assume bins1 and bins2 are sorted from smallest to largest
    assert len(col1) == len(col2)
    assert len(bins1) == len(bins2)
    n_samples = len(col1)
    n_bins = len(bins1)
    assume_dist = np.full((n_bins, n_bins), 1 / (n_bins * n_bins))
    real_dist = np.full((n_bins, n_bins), 0, dtype=np.int64)
    for val1, val2 in zip(col1, col2):
        i = max(bisect.bisect_left(bins1, val1) - 1, 0)
        j = max(bisect.bisect_left(bins2, val2) - 1, 0)
        real_dist[i, j] += 1
    real_dist = real_dist / n_samples
    # compute KL divergence
    kl = np.sum(real_dist * np.log(real_dist / assume_dist))
    return kl.item()


def test_chi_square(col1: List, bins1: List, col2: List, bins2: List) -> float:
    # assume bins1 and bins2 are sorted from smallest to largest
    assert len(col1) == len(col2)
    assert len(bins1) == len(bins2)
    n_samples = len(col1)
    n_bins = len(bins1)
    real_dist = np.full((n_bins, n_bins), 0, dtype=np.int64)
    for val1, val2 in zip(col1, col2):
        i = max(bisect.bisect_left(bins1, val1) - 1, 0)
        j = max(bisect.bisect_left(bins2, val2) - 1, 0)
        real_dist[i, j] += 1
    # compute chi square p-value
    _, p, *_ = chi2_contingency(real_dist, correction=False)
    return p

