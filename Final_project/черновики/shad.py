def floyd_warshall(n, g):
    INF = 1000000007
    for k in range(n):
        for i in range(n):
            for j in range(n):
                if g[i][k] != -1 and g[k][j] != -1:
                    g[i][j] = max(g[i][j], min(g[i][k], g[k][j]))
                    
    for i in range(n):
        for j in range(n):
            if g[i][j] == -1:
                g[i][j] = 0
            elif i == j:
                g[i][j] = INF
                
    return g

def main():
    n = int(input())
    g = [list(map(int, input().split())) for _ in range(n)]

    ans = floyd_warshall(n, g)

    for row in ans:
        print(' '.join(str(x) for x in row))

if __name__ == "__main__":
    main()
