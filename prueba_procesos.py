from multiprocessing import Pool


def f(x):
    return x*x


if __name__ == '__main__':
    with Pool(processes=4) as pool:
        result = pool.map(f, range(10))
        print(result)

    print('END.')
