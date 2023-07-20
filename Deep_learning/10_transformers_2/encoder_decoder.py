import numpy as np
import torch
import torch.nn as nn

from attention import Attention


def get_encoding(hidden_dim, max_length):
    pe = np.array(
        [
            [i / np.power(10000, 2 * (j // 2) / hidden_dim) for j in range(hidden_dim)]
            for i in range(max_length)
        ]
    )
    pe[:, 0::2] = np.sin(pe[:, 0::2])
    pe[:, 1::2] = np.cos(pe[:, 1::2])

    return torch.FloatTensor(pe).unsqueeze(0)


class PositionalEncoding(nn.Module):
    def __init__(self, hidden_dim, max_length=100):
        super().__init__()

        self.register_buffer("pe", get_encoding(hidden_dim, max_length))
        self.pe = self.pe.to(torch.device("cuda:0"))

    def forward(self, x):
        return x + self.pe[:, : x.size(1)]


class EncoderLayer(nn.Module):
    def __init__(
        self,
        hidden_dim: int,
        n_heads: int,
        dropout: float,
        forward_expansion: int,
    ):
        super().__init__()

        self.norm1 = nn.LayerNorm(hidden_dim)
        self.norm2 = nn.LayerNorm(hidden_dim)

        self.attention = Attention(hidden_dim, n_heads, dropout)

        self.feed_forward = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim * forward_expansion),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim * forward_expansion, hidden_dim),
        )

        self.dropout = nn.Dropout(dropout)

    def forward(self, x, mask):
        x = self.norm1(x + self.dropout(self.attention(x, x, x, mask)))

        return self.norm2(x + self.dropout(self.feed_forward(x)))


class Encoder(nn.Module):
    def __init__(
        self,
        input_dim: int,
        hidden_dim: int,
        n_layers: int,
        n_heads: int,
        forward_expansion: int,
        dropout: float,
        device: torch.device,
        pad_idx: int,
        max_length: int = 90,
    ):
        super().__init__()

        self.embedding = nn.Embedding(input_dim, hidden_dim, padding_idx=pad_idx)
        self.positional_encoding = PositionalEncoding(hidden_dim, max_length)

        self.layers = nn.ModuleList(
            [
                EncoderLayer(
                    hidden_dim=hidden_dim,
                    n_heads=n_heads,
                    dropout=dropout,
                    forward_expansion=forward_expansion,
                )
                for _ in range(n_layers)
            ]
        )

        self.scale = hidden_dim ** 0.5
        self.dropout = nn.Dropout(dropout)
        self.device = device

    def forward(self, x, mask):
        x = self.dropout(self.positional_encoding(self.embedding(x) * self.scale))

        for layer in self.layers:
            x = layer(x, mask)

        return x


class DecoderLayer(nn.Module):
    def __init__(
        self,
        hidden_dim: int,
        n_heads: int,
        dropout: float,
        forward_expansion: int,
    ):
        super().__init__()

        self.norm1 = nn.LayerNorm(hidden_dim)
        self.norm2 = nn.LayerNorm(hidden_dim)
        self.norm3 = nn.LayerNorm(hidden_dim)

        self.attention1 = Attention(hidden_dim, n_heads, dropout)
        self.attention2 = Attention(hidden_dim, n_heads, dropout)

        self.feed_forward = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim * forward_expansion),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim * forward_expansion, hidden_dim),
        )

        self.dropout = nn.Dropout(dropout)

    def forward(self, x, src_enc, src_mask, trg_mask):
        x = self.norm1(x + self.dropout(self.attention1(x, x, x, trg_mask)))

        x = self.norm2(x + self.dropout(self.attention2(x, src_enc, src_enc, src_mask)))

        return self.norm3(x + self.dropout(self.feed_forward(x)))


class Decoder(nn.Module):
    def __init__(
        self,
        output_dim: int,
        hidden_dim: int,
        n_layers: int,
        n_heads: int,
        forward_expansion: int,
        dropout: float,
        device: torch.device,
        pad_idx: int,
        max_length: int = 90,
    ):
        super().__init__()

        self.embedding = nn.Embedding(output_dim, hidden_dim, padding_idx=pad_idx)
        self.positional_encoding = PositionalEncoding(hidden_dim, max_length)

        self.layers = nn.ModuleList(
            [
                DecoderLayer(
                    hidden_dim=hidden_dim,
                    n_heads=n_heads,
                    dropout=dropout,
                    forward_expansion=forward_expansion,
                )
                for _ in range(n_layers)
            ]
        )

        self.fc = nn.Linear(hidden_dim, output_dim)

        self.scale = hidden_dim ** 0.5
        self.dropout = nn.Dropout(dropout)
        self.device = device

    def forward(self, x, enc_out, src_mask, trg_mask):
        x = self.dropout(self.positional_encoding(self.embedding(x) * self.scale))

        for layer in self.layers:
            x = layer(x, enc_out, src_mask, trg_mask)

        return self.fc(x)
