#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script de Verificação Rápida - Diagnóstico de Problemas
Verifica inconsistências em arquivos JSON de advogados processados
"""

import json
import sys
import os
from collections import defaultdict

def clean_and_validate_state(state):
    """Clean state field and validate if it's a valid Brazilian state"""
    if not state:
        return None
    
    # Remove caracteres inválidos e manter apenas letras
    cleaned = ''.join(c for c in str(state).upper() if c.isalpha())
    
    # Pegar apenas os primeiros 2 caracteres
    if len(cleaned) >= 2:
        cleaned = cleaned[:2]
    
    # Estados brasileiros válidos
    valid_states = {
        'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 
        'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 
        'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
    }
    
    if cleaned in valid_states:
        return cleaned
    
    return None

def extract_state_from_oab_id(oab_id):
    """Extract state from oab_id field (e.g., 'MG_185929' -> 'MG')"""
    if not oab_id:
        return None
    
    parts = str(oab_id).split('_')
    if len(parts) >= 2:
        estado_from_oab = clean_and_validate_state(parts[0])
        return estado_from_oab
    return None

def verify_lawyer_data(filename):
    """Verificar problemas nos dados dos advogados"""
    
    print(f"🔍 VERIFICANDO ARQUIVO: {os.path.basename(filename)}")
    print("=" * 80)
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            lawyers_data = json.load(f)
    except Exception as e:
        print(f"❌ ERRO ao ler arquivo: {e}")
        return
    
    total_records = len(lawyers_data)
    print(f"📊 Total de registros: {total_records}")
    print()
    
    # Contadores
    issues_found = 0
    state_inconsistent = []
    not_processed = []
    incomplete_societies = []
    missing_oab_id = []
    
    # Verificar cada advogado
    for i, lawyer in enumerate(lawyers_data):
        lawyer_id = lawyer.get('id', f'Index_{i}')
        full_name = lawyer.get('full_name', 'Nome_Desconhecido')
        oab_id = lawyer.get('oab_id')
        current_state = lawyer.get('state')
        
        # 1. VERIFICAR ESTADOS INCONSISTENTES
        if oab_id:
            estado_correto = extract_state_from_oab_id(oab_id)
            current_state_clean = clean_and_validate_state(current_state)
            
            if estado_correto and current_state_clean:
                # Ambos são estados válidos, comparar se são diferentes
                if current_state_clean != estado_correto:
                    state_inconsistent.append({
                        'id': lawyer_id,
                        'name': full_name,
                        'oab_id': oab_id,
                        'current_state': current_state,
                        'current_state_clean': current_state_clean,
                        'correct_state': estado_correto
                    })
                    issues_found += 1
            elif estado_correto and not current_state_clean:
                # Estado atual é inválido, mas oab_id tem estado válido
                state_inconsistent.append({
                    'id': lawyer_id,
                    'name': full_name,
                    'oab_id': oab_id,
                    'current_state': current_state,
                    'current_state_clean': 'INVÁLIDO',
                    'correct_state': estado_correto
                })
                issues_found += 1
        else:
            missing_oab_id.append({
                'id': lawyer_id,
                'name': full_name,
                'state': current_state
            })
            issues_found += 1
        
        # 2. VERIFICAR ADVOGADOS NÃO PROCESSADOS
        processed = lawyer.get('processed', False)
        if not processed:
            not_processed.append({
                'id': lawyer_id,
                'name': full_name,
                'state': current_state,
                'oab_id': oab_id
            })
            issues_found += 1
        
        # 3. VERIFICAR SOCIEDADES INCOMPLETAS
        has_society = lawyer.get('has_society', False)
        if has_society:
            society_basic = lawyer.get('society_basic_details', [])
            society_complete = lawyer.get('society_complete_details', [])
            
            if not society_basic or not society_complete:
                incomplete_societies.append({
                    'id': lawyer_id,
                    'name': full_name,
                    'state': current_state,
                    'oab_id': oab_id,
                    'has_basic': len(society_basic) > 0,
                    'has_complete': len(society_complete) > 0,
                    'basic_count': len(society_basic),
                    'complete_count': len(society_complete)
                })
                issues_found += 1
    
    # RELATÓRIO DE PROBLEMAS
    print("🚨 PROBLEMAS ENCONTRADOS:")
    print("=" * 50)
    
    if issues_found == 0:
        print("✅ NENHUM PROBLEMA ENCONTRADO! Arquivo está OK.")
        return
    
    print(f"⚠️  Total de problemas: {issues_found}")
    print()
    
    # 1. ESTADOS INCONSISTENTES
    if state_inconsistent:
        print(f"🔴 ESTADOS INCONSISTENTES: {len(state_inconsistent)} encontrados")
        print("-" * 30)
        for issue in state_inconsistent[:10]:  # Mostrar apenas os primeiros 10
            print(f"  📋 ID: {issue['id']} | {issue['name']}")
            print(f"      oab_id: {issue['oab_id']}")
            if issue['current_state_clean'] == 'INVÁLIDO':
                print(f"      Estado atual: '{issue['current_state']}' (INVÁLIDO) → Deveria ser: '{issue['correct_state']}'")
            else:
                print(f"      Estado atual: '{issue['current_state']}' (limpo: '{issue['current_state_clean']}') → Deveria ser: '{issue['correct_state']}'")
            print()
        
        if len(state_inconsistent) > 10:
            print(f"      ... e mais {len(state_inconsistent) - 10} registros com o mesmo problema")
        print()
    
    # 2. OAB_ID FALTANDO
    if missing_oab_id:
        print(f"🔴 OAB_ID FALTANDO: {len(missing_oab_id)} encontrados")
        print("-" * 30)
        for issue in missing_oab_id[:5]:  # Mostrar apenas os primeiros 5
            print(f"  📋 ID: {issue['id']} | {issue['name']} | Estado: {issue['state']}")
        
        if len(missing_oab_id) > 5:
            print(f"      ... e mais {len(missing_oab_id) - 5} registros sem oab_id")
        print()
    
    # 3. NÃO PROCESSADOS
    if not_processed:
        print(f"🔴 NÃO PROCESSADOS: {len(not_processed)} encontrados")
        print("-" * 30)
        for issue in not_processed[:10]:  # Mostrar apenas os primeiros 10
            print(f"  📋 ID: {issue['id']} | {issue['name']} | {issue['state']} | {issue['oab_id']}")
        
        if len(not_processed) > 10:
            print(f"      ... e mais {len(not_processed) - 10} registros não processados")
        print()
    
    # 4. SOCIEDADES INCOMPLETAS
    if incomplete_societies:
        print(f"🔴 SOCIEDADES INCOMPLETAS: {len(incomplete_societies)} encontrados")
        print("-" * 30)
        for issue in incomplete_societies[:10]:  # Mostrar apenas os primeiros 10
            basic_status = "✅" if issue['has_basic'] else "❌"
            complete_status = "✅" if issue['has_complete'] else "❌"
            print(f"  📋 ID: {issue['id']} | {issue['name']}")
            print(f"      Basic: {basic_status} ({issue['basic_count']}) | Complete: {complete_status} ({issue['complete_count']})")
            print()
        
        if len(incomplete_societies) > 10:
            print(f"      ... e mais {len(incomplete_societies) - 10} registros com sociedades incompletas")
        print()
    
    # RESUMO FINAL
    print("📊 RESUMO:")
    print("=" * 30)
    print(f"  • Total de registros: {total_records}")
    print(f"  • Estados inconsistentes: {len(state_inconsistent)}")
    print(f"  • OAB_ID faltando: {len(missing_oab_id)}")
    print(f"  • Não processados: {len(not_processed)}")
    print(f"  • Sociedades incompletas: {len(incomplete_societies)}")
    print(f"  • Total de problemas: {issues_found}")
    
    if issues_found > 0:
        percentage = (issues_found / total_records) * 100
        print(f"  • Percentual de problemas: {percentage:.1f}%")
    
    print()
    print("💡 RECOMENDAÇÃO:")
    if state_inconsistent:
        print("   ⚡ Execute o script de processamento para corrigir estados inconsistentes")
    if not_processed:
        print("   ⚡ Execute o script de processamento para processar registros pendentes")
    if incomplete_societies:
        print("   ⚡ Execute o script de processamento para completar dados de sociedades")

def main():
    """Função principal"""
    if len(sys.argv) != 2:
        print("❌ Uso: python verificacao_rapida.py <arquivo.json>")
        print("   Exemplo: python verificacao_rapida.py lawyers_001_v3.json")
        sys.exit(1)
    
    filename = sys.argv[1]
    
    if not os.path.exists(filename):
        print(f"❌ Arquivo não encontrado: {filename}")
        sys.exit(1)
    
    verify_lawyer_data(filename)

if __name__ == "__main__":
    main()