from torchvision import datasets, transforms
from torch.utils.data import DataLoader
from tqdm import tqdm

import torch.nn.functional as F
from torchvision.datasets import CIFAR10
from torchvision.transforms import ToTensor
from torch.optim import Adam
from torch import nn
import torch

def create_simple_conv_cifar() -> nn.Sequential:
    model = nn.Sequential(
        nn.Conv2d(in_channels=3, out_channels=16, kernel_size=3, padding=1),  # 32 x 32 x 16
        nn.ReLU(),
        nn.MaxPool2d(2),  # 16 x 16 x 16
        nn.Conv2d(in_channels=16, out_channels=32, kernel_size=3, padding=1),  # 16 x 16 x 32
        nn.ReLU(),
        nn.MaxPool2d(2),  # 8 x 8 x 32
        nn.Flatten(),
        nn.Linear(8 * 8 * 32, 1024),
        nn.ReLU(),
        nn.Linear(1024, 128),
        nn.ReLU(),
        nn.Linear(128, 10)
    )
    return model

def train(model, device, train_loader, optimizer, epoch):
    model.train()
    for batch_idx, (data, target) in enumerate(train_loader):
        data, target = data.to(device), target.to(device)
        optimizer.zero_grad()
        output = model(data)
        loss = F.cross_entropy(output, target)
        loss.backward()
        optimizer.step()

def test(model, device, test_loader):
    model.eval()
    correct = 0
    with torch.no_grad():
        for data, target in test_loader:
            data, target = data.to(device), target.to(device)
            output = model(data)
            pred = output.argmax(dim=1, keepdim=True)
            correct += pred.eq(target.view_as(pred)).sum().item()
    return correct / len(test_loader.dataset)

@torch.inference_mode()
def predict(model: nn.Module, loader: DataLoader, device: torch.device):
    model.eval()
    prediction = torch.empty(0, device=device)
    with torch.no_grad():
        for x, y in loader:
            x, y = x.to(device), y.to(device)

            output = model(x)
            pred = torch.argmax(output, dim=1)
            prediction = torch.cat((prediction, pred))
    return prediction

def main():
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model = create_simple_conv_cifar().to(device)
    optimizer = Adam(model.parameters())

    transform = transforms.Compose([transforms.ToTensor()])
    train_set = datasets.CIFAR10(root='./data', train=True, download=True, transform=transform)
    test_set = datasets.CIFAR10(root='./data', train=False, download=True, transform=transform)
    train_loader = DataLoader(train_set, batch_size=64, shuffle=True)
    test_loader = DataLoader(test_set, batch_size=1000, shuffle=False)

    for epoch in range(1, 30):
        train(model, device, train_loader, optimizer, epoch)
        accuracy = test(model, device, test_loader)
        print(f'Epoch: {epoch}, Accuracy: {accuracy}')
        if accuracy >= 0.9:
            break

    predictions = predict(model, test_loader, device)
    torch.save(predictions, 'predictions.pt')

main()