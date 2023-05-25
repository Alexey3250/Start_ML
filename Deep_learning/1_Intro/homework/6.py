import torch
import numpy as np

def function02(tensor: torch.tensor) -> torch.Tensor:
    # calculate the number of columns in the tensor
    num_features = tensor.size(1)
    
    # creating the tensor of weights with the same shape as the input tensor и разрешаем вычисление градиента
    weights = torch.rand(num_features, dtype=torch.float32, requires_grad=True)
       
    return weights