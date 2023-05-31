import os
import numpy as np
from matplotlib import pyplot as plt
from abc import ABCMeta, abstractmethod


class Optimization(metaclass=ABCMeta):
    def __init__(self, df: any, params: dict):
        if isinstance(df, list):
            self.df_list = df
        else:
            self.df = df
        self.params = params

    def __call__(self, trial):
        # ハイパーパラメータの設定
        config = {}
        for key, value in self.params.items():
            config[key] = trial.suggest_int(key, value[0], value[1], value[2])
        return self.indicator(**config)

    @abstractmethod
    def indicator(self, **kwargs):
        raise NotImplementedError


def simple_regression(x: np.ndarray, y: np.ndarray, plot_graph=False, title: str = "Linear Regression",
                      x_label: str = "x", y_label: str = "y", output_dir: str = None, save_fig: bool = False):

    ic = np.corrcoef(x, y)[0, 1]
    r2 = ic ** 2
    if r2 == np.nan:
        r2 = 0

    if not plot_graph:
        return r2

    N = len(x)
    p, cov, _ = np.polyfit(x, y, 1, cov=True)
    a = p[0]
    b = p[1]
    sigma_a = np.sqrt(cov[0, 0])
    sigma_b = np.sqrt(cov[1, 1])
    sigma_y = np.sqrt(1 / (N - 2) * np.sum([(a * xi + b - yi) ** 2 for xi, yi in zip(x, y)]))
    yy = a * x + b
    fig = plt.figure()
    fig.suptitle(f"{title} IC={ic}")
    ax = fig.add_subplot(111)
    ax.scatter(x, y, c="blue", s=20, edgecolors="blue", alpha=0.3)
    ax.plot(x, yy, color='r')
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.grid(which="major", axis="x", color="gray", alpha=0.5, linestyle="dotted", linewidth=1)
    ax.grid(which="major", axis="y", color="gray", alpha=0.5, linestyle="dotted", linewidth=1)
    ax.text(1.02, 0.04, f"y = {a:.3f} ± {sigma_a:.3f}x + {b:.3f} ± {sigma_b:.3f}\nsigma_y={sigma_y:.3f}\nlength={x.size}", transform=ax.transAxes)
    ax.text(0.788, 0.1, f"R**2={r2:.4f}", transform=ax.transAxes)
    ax.text(0.59, 0.04, f"ProportionCorrect={(np.sqrt(r2) + 1) / 2 * 100:.2f}%", transform=ax.transAxes)

    if save_fig:
        if output_dir is None:
            output_dir = f'./png'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        plt.savefig(f'{output_dir}/{title}.png')
    plt.show()