const fs = require('fs');
const path = require('path');

// Diretório onde estão os arquivos (diretório atual)
const inputDir = process.cwd();

// Lista dos arquivos na ordem correta (nomes exatos do ls)
const fileNames = [
    'lawyers_enhanced_lawyers_028-cagado_parte_01_FINAL_20250701_221143.json',
    'lawyers_enhanced_lawyers_028-cagado_parte_02_FINAL_20250701_220844.json',
    'lawyers_enhanced_lawyers_028-cagado_parte_03_FINAL_20250701_221103.json',
    'lawyers_enhanced_lawyers_028-cagado_parte_04_FINAL_20250701_221249.json',
    'lawyers_enhanced_lawyers_028-cagado_parte_05_FINAL_20250701_221134.json',
    'lawyers_enhanced_lawyers_028-cagado_parte_06_FINAL_20250702_000153.json',
    'lawyers_enhanced_lawyers_028-cagado_parte_07_FINAL_20250702_000439.json',
    'lawyers_enhanced_lawyers_028-cagado_parte_08_FINAL_20250702_000130.json',
    'lawyers_enhanced_lawyers_028-cagado_parte_09_FINAL_20250702_000530.json',
    'lawyers_enhanced_lawyers_028-cagado_parte_10_FINAL_20250701_234954.json'
];

// Função para juntar os arquivos JSON
function mergeJsonFiles() {
    try {
        console.log('🔄 Iniciando junção dos arquivos JSON...\n');
        
        let mergedData = [];
        let totalObjects = 0;
        
        // Processa cada arquivo na ordem
        for (let i = 0; i < fileNames.length; i++) {
            const fileName = fileNames[i];
            const filePath = path.join(inputDir, fileName);
            
            console.log(`📖 Lendo parte ${i + 1}: ${fileName}`);
            
            // Verifica se o arquivo existe
            if (!fs.existsSync(filePath)) {
                console.error(`❌ Arquivo não encontrado: ${fileName}`);
                continue;
            }
            
            // Lê e parseia o JSON
            const fileContent = fs.readFileSync(filePath, 'utf8');
            const jsonData = JSON.parse(fileContent);
            
            // Verifica se é um array
            if (!Array.isArray(jsonData)) {
                console.error(`❌ Arquivo ${fileName} não contém um array válido`);
                continue;
            }
            
            // Adiciona os dados ao array principal
            mergedData = mergedData.concat(jsonData);
            totalObjects += jsonData.length;
            
            console.log(`   ✅ ${jsonData.length} objetos adicionados`);
        }
        
        console.log(`\n📊 Total de objetos processados: ${totalObjects}`);
        
        // Gera nome do arquivo de saída com timestamp
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
        const outputFileName = `lawyers_028-cagado_MERGED_${timestamp}.json`;
        const outputPath = path.join(inputDir, outputFileName);
        
        // Salva o arquivo final
        console.log(`💾 Salvando arquivo final: ${outputFileName}`);
        fs.writeFileSync(outputPath, JSON.stringify(mergedData, null, 2));
        
        console.log('\n✅ Junção concluída com sucesso!');
        console.log(`📁 Arquivo salvo em: ${outputPath}`);
        console.log(`📈 Total final: ${mergedData.length} objetos`);
        
        // Verifica integridade
        console.log('\n🔍 Verificação de integridade:');
        console.log(`   - Objetos esperados: ~6515`);
        console.log(`   - Objetos obtidos: ${mergedData.length}`);
        console.log(`   - Status: ${mergedData.length >= 6500 ? '✅ OK' : '⚠️  Verificar'}`);
        
    } catch (error) {
        console.error('❌ Erro ao processar os arquivos:', error.message);
        console.error(error.stack);
    }
}

// Função alternativa que busca automaticamente os arquivos
function mergeJsonFilesAuto() {
    try {
        console.log('🔍 Buscando arquivos automaticamente...\n');
        
        // Lê todos os arquivos do diretório
        const allFiles = fs.readdirSync(inputDir);
        
        // Filtra apenas os arquivos FINAL da sequência
        const finalFiles = allFiles
            .filter(file => file.includes('lawyers_enhanced_lawyers_028-cagado_parte_') && file.includes('_FINAL_') && file.endsWith('.json'))
            .sort((a, b) => {
                // Extrai o número da parte para ordenar corretamente
                const aNum = parseInt(a.match(/parte_(\d+)/)[1]);
                const bNum = parseInt(b.match(/parte_(\d+)/)[1]);
                return aNum - bNum;
            });
        
        console.log(`📁 Encontrados ${finalFiles.length} arquivos:`);
        finalFiles.forEach((file, index) => {
            console.log(`   ${index + 1}. ${file}`);
        });
        
        let mergedData = [];
        let totalObjects = 0;
        
        // Processa cada arquivo
        for (const fileName of finalFiles) {
            const filePath = path.join(inputDir, fileName);
            
            console.log(`\n📖 Processando: ${fileName}`);
            
            const fileContent = fs.readFileSync(filePath, 'utf8');
            const jsonData = JSON.parse(fileContent);
            
            if (Array.isArray(jsonData)) {
                mergedData = mergedData.concat(jsonData);
                totalObjects += jsonData.length;
                console.log(`   ✅ ${jsonData.length} objetos adicionados`);
            } else {
                console.log(`   ⚠️  Arquivo não contém array válido`);
            }
        }
        
        // Salva resultado
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
        const outputFileName = `lawyers_028-cagado_MERGED_AUTO_${timestamp}.json`;
        const outputPath = path.join(inputDir, outputFileName);
        
        fs.writeFileSync(outputPath, JSON.stringify(mergedData, null, 2));
        
        console.log(`\n✅ Junção automática concluída!`);
        console.log(`📁 Arquivo: ${outputFileName}`);
        console.log(`📈 Total: ${mergedData.length} objetos`);
        
    } catch (error) {
        console.error('❌ Erro na junção automática:', error.message);
    }
}

// Executa ambas as funções
console.log('='.repeat(60));
console.log('🔧 MÉTODO 1: Junção com lista específica');
console.log('='.repeat(60));
mergeJsonFiles();

console.log('\n' + '='.repeat(60));
console.log('🔧 MÉTODO 2: Junção automática (busca arquivos)');
console.log('='.repeat(60));
mergeJsonFilesAuto();