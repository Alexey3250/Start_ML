import torch
import torch.nn as nn


BIAS: bool = True


class Attention(nn.Module):
    def __init__(self, hidden_dim: int, n_heads: int, dropout: float):
        super().__init__()

        assert (
            hidden_dim % n_heads == 0
        ), "Hidden size should be divisible by number of heads"

        self.hidden_dim = hidden_dim
        self.n_heads = n_heads
        self.head_dim = hidden_dim // n_heads

        self.scale = hidden_dim ** (1 / 2)

        self.v = nn.Linear(hidden_dim, hidden_dim, bias=BIAS)
        self.k = nn.Linear(hidden_dim, hidden_dim, bias=BIAS)
        self.q = nn.Linear(hidden_dim, hidden_dim, bias=BIAS)

        self.fc = nn.Linear(hidden_dim, hidden_dim, bias=BIAS)
        # https://pytorch.org/docs/stable/generated/torch.nn.LayerNorm.html
        self.norm = nn.LayerNorm(hidden_dim)

        self.dropout = nn.Dropout(dropout)

    def forward(self, q, k, v, mask=None):
        residual = q

        batch_size = q.shape[0]

        q = self.q(q).reshape(batch_size, -1, self.n_heads, self.head_dim)
        k = self.k(k).reshape(batch_size, -1, self.n_heads, self.head_dim)
        v = self.v(v).reshape(batch_size, -1, self.n_heads, self.head_dim)

        # https://pytorch.org/docs/stable/generated/torch.einsum.html
        energy = torch.einsum("nqhd,nkhd->nhqk", q, k)

        if mask is not None:
            energy = energy.masked_fill(mask == 0, -1e9)

        attention = torch.softmax(energy / self.scale, dim=3)

        out = torch.einsum("nhqk,nkhd->nqhd", self.dropout(attention), v).reshape(
            batch_size, -1, self.hidden_dim
        )

        return self.norm(self.fc(out) + residual)
