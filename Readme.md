# Residential Proxy PoC

Sistema de proxy residencial próprio, usando máquinas de parceiros
como saída de internet, via túnel reverso (relay + agente).

## Estrutura

- `relay/` — servidor que roda na nuvem (EC2), aceita conexões de agentes
  e expõe um proxy HTTP pro robô usar.
- `agent/` — programa que roda na máquina do cliente, conecta no relay
  e repassa tráfego pela internet local dessa máquina.

## Status

PoC funcional, validado com múltiplos agentes e round-robin.
Próximos passos: dashboard web, token único por cliente, interface do agente.