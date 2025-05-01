from typing import List
import numpy as np
import bisect
from scipy.stats import chi2_contingency, spearmanr, pearsonr
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


def get_contingency_table(col1: List, bins1: List, col2: List, bins2: List) -> np.array:
    n_bins1, n_bins2 = len(bins1), len(bins2)
    real_dist = np.full((n_bins1, n_bins2), 0, dtype=np.float64)
    for val1, val2 in zip(col1, col2):
        i_min, i_max = bisect.bisect_left(bins1, val1), bisect.bisect_right(bins1, val1)
        j_min, j_max = bisect.bisect_left(bins2, val2), bisect.bisect_right(bins2, val2)
        area = (i_max - i_min) * (j_max - j_min)
        if area == 0:
            i_min = min(i_min, len(bins1) - 1)
            j_min = min(j_min, len(bins2) - 1)
            real_dist[i_min, j_min] += 1
        else:
            real_dist[i_min:i_max, j_min:j_max] += 1 / area
    return real_dist


def test_kl_divergence(col1: List, bins1: List, col2: List, bins2: List) -> float:
    # assume bins1 and bins2 are sorted from smallest to largest
    n_samples = len(col1)
    n_bins1, n_bins2 = len(bins1), len(bins2)
    assume_dist = np.full((n_bins1, n_bins2), 1 / (n_bins1 * n_bins2))
    real_dist = get_contingency_table(col1, bins1, col2, bins2)
    real_dist = real_dist / n_samples + 1e-9
    # compute KL divergence
    kl = np.sum(real_dist * np.log(real_dist / assume_dist))
    return kl.item()


def test_chi_square(col1: List, bins1: List, col2: List, bins2: List) -> float:
    # assume bins1 and bins2 are sorted from smallest to largest
    real_dist = get_contingency_table(col1, bins1, col2, bins2)
    real_dist += 1
    # compute chi square p-value
    _, p, *_ = chi2_contingency(real_dist, correction=False)
    return p


def test_spearman(col1: List, bins1: List, col2: List, bins2: List) -> float:
    real_dist = get_contingency_table(col1, bins1, col2, bins2).astype(np.int64)
    # Convert contingency table to paired observations
    pairs = []
    for i in range(real_dist.shape[0]):
        for j in range(real_dist.shape[1]):
            count = real_dist[i, j]
            # Add the pair (i, j) 'count' times to the pairs list
            pairs.extend([(i, j)] * count)

    # Convert to numpy arrays
    pairs = np.array(pairs)
    x = pairs[:, 0]
    y = pairs[:, 1]

    # Calculate Spearman correlation
    correlation, p_value = spearmanr(x, y)
    return p_value


def test_pearson(col1: List, bins1: List, col2: List, bins2: List) -> float:
    real_dist = get_contingency_table(col1, bins1, col2, bins2).astype(np.int64)
    # Convert contingency table to paired observations
    pairs = []
    for i in range(real_dist.shape[0]):
        for j in range(real_dist.shape[1]):
            count = real_dist[i, j]
            # Add the pair (i, j) 'count' times to the pairs list
            pairs.extend([(i, j)] * count)

    # Convert to numpy arrays
    pairs = np.array(pairs)
    x = pairs[:, 0]
    y = pairs[:, 1]

    # Calculate Pearson correlation
    correlation, p_value = pearsonr(x, y)
    return p_value