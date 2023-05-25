import torch
import numpy as np

def function01(tensor: torch.tensor, count_over: str) -> torch.Tensor:
    if count_over == "columns":
        return torch.mean(tensor, dim=0)
    else:
        return torch.mean(tensor, dim=1)
