import torchvision.transforms as T

def get_augmentations(train: bool = True) -> T.Compose:
    means = (0.49139968, 0.48215841, 0.44653091)
    stds = (0.24703223, 0.24348513, 0.26158784)

    if train:
        return T.Compose([
            T.Resize((224, 224)),
            T.RandomHorizontalFlip(),
            T.ToTensor(),
            T.Normalize(means, stds)
        ])
    else:
        return T.Compose([
            T.Resize((224, 224)),
            T.ToTensor(),
            T.Normalize(means, stds)
        ])