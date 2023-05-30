from torch import nn
import torch
from torch.utils.data import DataLoader

@torch.inference_mode()
def predict(model: nn.Module, loader: DataLoader, device: torch.device):
    model.eval()
    prediction = torch.empty(0)
    with torch.no_grad():
        for x, y in loader:
            x, y = x.to(device), y.to(device)

            output = model(x)
            pred = torch.argmax(output, dim=1)
            prediction = torch.cat((prediction, pred))
    return prediction