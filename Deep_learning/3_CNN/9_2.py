import torch
import torch.nn as nn
import torch.optim as optim
from torchvision import datasets, transforms
from torch.utils.data import DataLoader
import torchvision
from tqdm import tqdm
from torchvision.datasets import MNIST
import torchvision.transforms as T



def create_conv_model():
    model = nn.Sequential(
        nn.Conv2d(in_channels=1, out_channels=64, kernel_size=3, padding=1),
        nn.ReLU(),
        nn.Conv2d(in_channels=64, out_channels=64, kernel_size=3, padding=1),
        nn.ReLU(),
        nn.MaxPool2d(kernel_size=2),

        nn.Conv2d(in_channels=64, out_channels=128, kernel_size=3, padding=1),
        nn.ReLU(),
        nn.Conv2d(in_channels=128, out_channels=128, kernel_size=3, padding=1),
        nn.ReLU(),
        nn.MaxPool2d(kernel_size=2),

        nn.Conv2d(in_channels=128, out_channels=256, kernel_size=3, padding=1),
        nn.ReLU(),
        nn.Conv2d(in_channels=256, out_channels=256, kernel_size=3, padding=1),
        nn.ReLU(),
        nn.MaxPool2d(kernel_size=2),

        nn.Flatten(),
        nn.Linear(3 * 3 * 256, 256),
        nn.ReLU(),
        nn.Linear(256, 10)
    )
    return model

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

def train(model: nn.Module, device: torch.device, data_loader: DataLoader, optimizer: torch.optim.Optimizer, loss_fn):
    model.train()
    total_loss = 0
    total = 0
    correct = 0
    for x, y in tqdm(data_loader, desc='Train'):
        x = x.to(device)
        y = y.to(device)
        optimizer.zero_grad()
        output = model(x)
        loss = loss_fn(output, y)
        loss.backward()
        total_loss += loss.item()
        optimizer.step()
        _, y_pred = torch.max(output, 1)
        total += y.size(0)
        correct += (y_pred == y).sum().item()
    return total_loss / len(data_loader), correct / total

def evaluate(model: nn.Module, device: torch.device, data_loader: DataLoader, loss_fn):
    model.eval()
    total_loss = 0
    total = 0
    correct = 0
    for x, y in tqdm(data_loader, desc='Evaluate'):
        x = x.to(device)
        y = y.to(device)
        output = model(x)
        loss = loss_fn(output, y)
        total_loss += loss.item()
        _, y_pred = torch.max(output, 1)
        total += y.size(0)
        correct += (y_pred == y).sum().item()
    return total_loss / len(data_loader), correct / total

model = create_conv_model().to(device)
loss_fn = nn.CrossEntropyLoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

transform = torchvision.transforms.Compose([torchvision.transforms.ToTensor(), torchvision.transforms.Normalize((0.5,), (0.5,))])

mnist_train = MNIST(
    "../datasets/mnist",
    train=True,
    download=True,
    transform=T.ToTensor()
)
mnist_valid = MNIST(
    "../datasets/mnist",
    train=False,
    download=True,
    transform=T.ToTensor()
)
# batch size is ho

trainloader = DataLoader(mnist_train, batch_size=64, shuffle=True)
testloader = DataLoader(mnist_valid, batch_size=64, shuffle=False)

for epoch in range(100):
    train_loss, train_accuracy = train(model, device, trainloader, optimizer, loss_fn)
    print(f'Epoch {epoch+1}, Train Loss: {train_loss}, Train Accuracy: {train_accuracy}')
    test_loss, test_accuracy = evaluate(model, device, testloader, loss_fn)
    print(f'Epoch {epoch+1}, Test Loss: {test_loss}, Test Accuracy: {test_accuracy}')
    if test_accuracy > 0.993:
        break

torch.save(model.state_dict(), 'mnist_model.pth')