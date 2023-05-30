import torch

def get_normalize(features: torch.Tensor):
    mean = features.mean(dim=(0, 2, 3))
    std = features.std(dim=(0, 2, 3))
    return mean, std