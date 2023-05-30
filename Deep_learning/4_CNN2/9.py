import torch
from torch import nn
from torch.utils.data import DataLoader

def predict_tta(model: nn.Module, loader: DataLoader, device: torch.device, iterations: int = 2):
    model.eval()
    with torch.no_grad():
        predictions = []
        for _ in range(iterations):
            single_prediction = []
            for inputs in loader:
                inputs = inputs.to(device)
                outputs = model(inputs)
                single_prediction.append(outputs)
            predictions.append(torch.vstack(single_prediction))
        predictions = torch.stack(predictions, dim=2)
        predictions = torch.mean(predictions, dim=2)
        classes = torch.argmax(predictions, dim=1)
    return classes