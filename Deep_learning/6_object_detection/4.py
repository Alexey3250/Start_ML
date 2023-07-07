import argparse
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader
from torchvision.datasets import OxfordIIITPet
from torchvision.transforms import Compose, Resize, ToTensor, Lambda
import torchvision.transforms as T
import logging
import os


# Disable SSL certificate verification
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# Define the UNet model with additional blocks and increased base_channels
class UNet(nn.Module):
    def __init__(self, in_channels, out_channels, base_channels):
        super(UNet, self).__init__()
        self.encoder1 = self.down_block(in_channels, base_channels)
        self.encoder2 = self.down_block(base_channels, base_channels * 2)
        self.encoder3 = self.down_block(base_channels * 2, base_channels * 4)
        self.encoder4 = self.down_block(base_channels * 4, base_channels * 8)

        self.bottleneck = nn.Sequential(
            nn.Conv2d(base_channels * 8, base_channels * 16, kernel_size=3, padding=1),
            nn.ReLU(inplace=True),
            nn.Conv2d(base_channels * 16, base_channels * 16, kernel_size=3, padding=1),
            nn.ReLU(inplace=True),
            nn.MaxPool2d(kernel_size=2, stride=2)
        )

        self.upconv4 = nn.ConvTranspose2d(base_channels * 16, base_channels * 8, kernel_size=2, stride=2)
        self.decoder4 = self.up_block(base_channels * 16, base_channels * 8)
        self.upconv3 = nn.ConvTranspose2d(base_channels * 8, base_channels * 4, kernel_size=2, stride=2)
        self.decoder3 = self.up_block(base_channels * 8, base_channels * 4)
        self.upconv2 = nn.ConvTranspose2d(base_channels * 4, base_channels * 2, kernel_size=2, stride=2)
        self.decoder2 = self.up_block(base_channels * 4, base_channels * 2)
        self.upconv1 = nn.ConvTranspose2d(base_channels * 2, base_channels, kernel_size=2, stride=2)
        self.decoder1 = self.up_block(base_channels * 2, base_channels)

        self.final_conv = nn.Conv2d(base_channels, out_channels, kernel_size=1)

    def down_block(self, in_channels, out_channels):
        return nn.Sequential(
            nn.Conv2d(in_channels, out_channels, kernel_size=3, padding=1),
            nn.ReLU(inplace=True),
            nn.Conv2d(out_channels, out_channels, kernel_size=3, padding=1),
            nn.ReLU(inplace=True),
            nn.MaxPool2d(kernel_size=2, stride=2)
        )

    def up_block(self, in_channels, out_channels):
        return nn.Sequential(
            nn.Conv2d(in_channels, out_channels, kernel_size=3, padding=1),
            nn.ReLU(inplace=True),
            nn.Conv2d(out_channels, out_channels, kernel_size=3, padding=1),
            nn.ReLU(inplace=True)
        )

    def forward(self, x):
        x1 = self.encoder1(x)
        x2 = self.encoder2(x1)
        x3 = self.encoder3(x2)
        x4 = self.encoder4(x3)

        bottleneck = self.bottleneck(x4)

        x = self.upconv4(bottleneck)
        x = torch.cat([x4, x], dim=1)
        x = self.decoder4(x)
        x = self.upconv3(x)
        x = torch.cat([x3, x], dim=1)
        x = self.decoder3(x)
        x = self.upconv2(x)
        x = torch.cat([x2, x], dim=1)
        x = self.decoder2(x)
        x = self.upconv1(x)
        x = torch.cat([x1, x], dim=1)
        x = self.decoder1(x)

        return self.final_conv(x)



# Define the transform for images
transform = T.Compose([
    T.Resize((256, 256)),
    T.ToTensor(),
])

def target_transform_fn(x):
    return (x-1).long()

target_transform = T.Compose([
    T.Resize((256, 256)),
    T.PILToTensor(),
    T.Lambda(target_transform_fn)
])

logging.basicConfig(level=logging.INFO)

# Define the accuracy function
def calculate_accuracy(outputs, targets):
    _, predicted = torch.max(outputs, 1)
    total_pixels = targets.numel()
    correct_pixels = (predicted == targets).sum().item()
    accuracy = correct_pixels / total_pixels
    return accuracy

def main(args):
    # Define the datasets and data loaders
    train_dataset = OxfordIIITPet(args.data_dir, transform=transform, target_transform=target_transform, target_types='segmentation', download=True)
    valid_dataset = OxfordIIITPet(args.data_dir, transform=transform, split='test', target_transform=target_transform, target_types='segmentation', download=True)
    train_loader = DataLoader(train_dataset, batch_size=64, shuffle=True, num_workers=4, pin_memory=True)
    valid_loader = DataLoader(valid_dataset, batch_size=64, shuffle=False, num_workers=4, pin_memory=True)

    # Define the UNet model
    model = UNet(args.in_channels, args.out_channels, args.base_channels)

    # Load weights if the file is specified
    if args.weights is not None and os.path.exists(args.weights):
        model.load_state_dict(torch.load(args.weights))
        logging.info(f"Loaded weights from {args.weights}")

    # Define the loss function and optimizer
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=args.learning_rate)

    # Train the model
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)

    for epoch in range(args.epochs):
        # Training
        model.train()
        for images, targets in train_loader:
            images = images.to(device)
            targets = targets.to(device)

            optimizer.zero_grad()

            outputs = model(images)
            loss = criterion(outputs, targets.squeeze(1))

            loss.backward()
            optimizer.step()

        # Validation
        model.eval()

        with torch.no_grad():
            total_accuracy = sum(calculate_accuracy(model(images.to(device)), targets.to(device)) for images, targets in valid_loader)

        logging.info(f"Epoch [{epoch+1}/{args.epochs}], Accuracy: {total_accuracy/len(valid_loader):.4f}")

    # Save the model weights
    torch.save(model.state_dict(), args.weights)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Train a U-Net model.')
    parser.add_argument('--data_dir', type=str, default='./data', help='Path to the dataset directory.')
    parser.add_argument('--weights', type=str, default='./weights.pt', help='Path to the weights file.')
    parser.add_argument('--in_channels', type=int, default=3, help='Number of input channels.')
    parser.add_argument('--out_channels', type=int, default=1, help='Number of output channels.')
    parser.add_argument('--base_channels', type=int, default=64, help='Number of base channels.')
    parser.add_argument('--epochs', type=int, default=10, help='Number of epochs.')
    parser.add_argument('--learning_rate', type=float, default=0.001, help='Learning rate.')
    args = parser.parse_args()

    main(args)