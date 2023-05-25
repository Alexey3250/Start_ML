import torch
import torch.nn as nn


def function04(x: torch.Tensor, y: torch.Tensor):
    layer = nn.Linear(x.shape[1], 1, bias=True)

    mse = torch.tensor([100])

    while mse > 0.3:
        mse = torch.mean((layer(x).ravel() - y) ** 2)

        mse.backward()

        with torch.no_grad():
            layer.weight -= layer.weight.grad * 1e-2
            layer.bias -= layer.bias.grad * 1e-2

        layer.zero_grad()

    return layer