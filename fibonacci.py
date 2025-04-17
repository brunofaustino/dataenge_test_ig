def fibonacci(n):
    """
    Calcula a sequência de Fibonacci até o n-ésimo número.
    
    Args:
        n (int): Número de elementos da sequência a serem calculados
        
    Returns:
        list: Lista contendo a sequência de Fibonacci
    """
    if n <= 0:
        return []
    elif n == 1:
        return [0]
    
    sequence = [0, 1]
    while len(sequence) < n:
        sequence.append(sequence[-1] + sequence[-2])
    
    return sequence

def main():
    # Exemplo de uso
    n = 10
    result = fibonacci(n)
    print(f"Sequência de Fibonacci com {n} elementos:")
    print(result)

if __name__ == "__main__":
    main() 