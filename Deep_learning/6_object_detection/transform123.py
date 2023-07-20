import torch
from torch.utils.data import Dataset, DataLoader
from torchvision import transforms as T
from PIL import Image
import glob
import os

class CustomOxfordIIITPet(Dataset):
    def __init__(self, root_dir, transform=None, target_transform=None):
        self.root_dir = root_dir
        self.transform = transform
        self.target_transform = target_transform

        self.image_paths = sorted(glob.glob(os.path.join(root_dir, 'images', '*.jpg')))
        self.annotation_paths = sorted(glob.glob(os.path.join(root_dir, 'annotations', 'trimaps', '*.png')))

    def __getitem__(self, idx):
        image_path = self.image_paths[idx]
        annotation_path = self.annotation_paths[idx]

        image = Image.open(image_path)
        annotation = Image.open(annotation_path)

        if self.transform:
            image = self.transform(image)
        if self.target_transform:
            annotation = self.target_transform(annotation)

        return image, annotation

    def __len__(self):
        return len(self.image_paths)

# Define the transforms in the global scope
transform = T.Compose(
    [
        T.Resize((256, 256)),
        T.ToTensor(),
    ]
)

def minus_one(x):
    return (x - 1).long()

target_transform = T.Compose(
    [
        T.Resize((256, 256)),
        T.PILToTensor(),
        T.Lambda(minus_one)
    ]
)

def transform_data(path, batch_size=32, num_workers=2, pin_memory=True):
    train_dataset = CustomOxfordIIITPet(path, transform=transform, target_transform=target_transform)
    valid_dataset = CustomOxfordIIITPet(path, transform=transform, target_transform=target_transform)

    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True, num_workers=num_workers, pin_memory=pin_memory)
    valid_loader = DataLoader(valid_dataset, batch_size=batch_size, shuffle=False, num_workers=num_workers, pin_memory=pin_memory)

    return train_loader, valid_loader
