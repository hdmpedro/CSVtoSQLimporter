package io.hdmpedro;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CSVImporter implements AutoCloseable {
    private static final Pattern INTEGER_PATTERN = Pattern.compile("^-?\\d+$");
    private static final Pattern DECIMAL_PATTERN = Pattern.compile("^-?\\d*\\.\\d+$");
    private static final Pattern DATE_PATTERN_BR = Pattern.compile("^\\d{2}/\\d{2}/\\d{4}$");
    private static final Pattern DATE_PATTERN_ISO = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}(\\s\\d{2}:\\d{2}:\\d{2})?$");
    private static final Pattern BOOLEAN_PATTERN = Pattern.compile("^(true|false|0|1|yes|no|y|n)$", Pattern.CASE_INSENSITIVE);
    private static final int BATCH_SIZE = 1000;
    private static final int SAMPLE_SIZE = 1000;
    private Map<String, String> tiposPersonalizados = new HashMap<>();

    private final String jdbcUrl;
    private final String usuario;
    private final String senha;
    private Connection connection;

    public CSVImporter(String jdbcUrl, String usuario, String senha) throws SQLException {
        this.jdbcUrl = jdbcUrl;
        this.usuario = usuario;
        this.senha = senha;
        this.tiposPersonalizados = tiposPersonalizados != null ? new HashMap<>(tiposPersonalizados) : new HashMap<>();
        this.connection = DriverManager.getConnection(jdbcUrl, usuario, senha);
    }

    public void definirTipoPersonalizado(String nomeColuna, String tipoSQL) {
        tiposPersonalizados.put(nomeColuna.trim(), tipoSQL.toUpperCase());
    }

    public void definirTiposPersonalizados(Map<String, String> tipos) {
        tipos.forEach((coluna, tipo) -> definirTipoPersonalizado(coluna, tipo));
    }

    private Map<String, String> aplicarTiposPersonalizados(Map<String, String> tiposDetectados) {
        Map<String, String> tiposFinais = new HashMap<>(tiposDetectados);

        tiposPersonalizados.forEach((nomeColuna, tipoPersonalizado) -> {
            if (tiposFinais.containsKey(nomeColuna)) {
                System.out.println("OVERRIDE: Coluna '" + nomeColuna + "' alterada de " +
                        tiposFinais.get(nomeColuna) + " pa " + tipoPersonalizado);
                tiposFinais.put(nomeColuna, tipoPersonalizado);
            } else {
                System.out.println("⚠️  AVISO: Coluna '" + nomeColuna + "' não encontrada para override");
            }
        });

        return tiposFinais;
    }

    public void importarCSV(String caminhoCsv, String nomeTabela, Set<String> colunasExcluidas,
                            Map<String, String> tiposPersonalizados) throws Exception {

        if (tiposPersonalizados != null) {
            tiposPersonalizados.forEach((coluna, tipo) -> definirTipoPersonalizado(coluna, tipo));
        }

        List<String> linhas = lerArquivoCsv(caminhoCsv);
        if (linhas.isEmpty()) throw new IllegalArgumentException("CSV estar vazio");

        char separador = detectarSeparador(linhas.get(0));
        String[] cabecalhos = parseCSVLinha(linhas.get(0), separador);

        List<String> cabecalhosFiltrados = Arrays.stream(cabecalhos)
                .map(String::trim)
                .filter(h -> !h.isEmpty() && !colunasExcluidas.contains(h))
                .collect(Collectors.toList());

        if (cabecalhosFiltrados.isEmpty()) {
            throw new IllegalArgumentException("Nenhuma coluna válida encontrada no CSV");
        }

        Map<String, String> columnTypes = detectarTiposColunas(linhas, cabecalhos, colunasExcluidas, separador);
        columnTypes = aplicarTiposPersonalizados(columnTypes);

        System.out.println("Colunas dedectadas: " + cabecalhosFiltrados.size());
        Map<String, String> finalColumnTypes = columnTypes;
        cabecalhosFiltrados.forEach(col ->
                System.out.println("   - " + col + " : " + finalColumnTypes.get(col))
        );

        criarTabela(nomeTabela, cabecalhosFiltrados, columnTypes);
        inserirDados(linhas.subList(1, linhas.size()), cabecalhos, cabecalhosFiltrados, nomeTabela, separador);
    }

    private static Map<String, String> parseArgumentosTipos(String[] argumentos, int inicioTipos) {
        Map<String, String> tipos = new HashMap<>();

        for (int i = inicioTipos; i < argumentos.length; i++) {
            String arg = argumentos[i];

            if (arg.contains(":")) {
                String[] partes = arg.split(":", 2);
                if (partes.length == 2) {
                    String coluna = partes[0].trim();
                    String tipo = partes[1].trim().toUpperCase();
                    tipos.put(coluna, tipo);
                    System.out.println(" Tipo personalizado definido: " + coluna + " -> " + tipo);
                }
            }
        }

        return tipos;
    }

    public void importarCSV(String caminhoCsv, String nomeTabela, Set<String> colunasExcluidas) throws Exception {
        importarCSV(caminhoCsv, nomeTabela, colunasExcluidas, null);
    }

    public void adicionarDadosTabela(String caminhoCsv, String nomeTabela) throws Exception {
        if (!verificarTabelaExiste(nomeTabela)) {
            throw new IllegalArgumentException("Tabela " + nomeTabela + " não existe");
        }

        List<String> linhas = lerArquivoCsv(caminhoCsv);
        if (linhas.isEmpty()) throw new IllegalArgumentException("CSV file is empty");

        char separador = detectarSeparador(linhas.get(0));
        String[] cabecalhos = parseCSVLinha(linhas.get(0), separador);
        List<String> cabecalhosFiltrados = Arrays.asList(cabecalhos).stream()
                .map(String::trim)
                .filter(h -> !h.isEmpty())

                .collect(Collectors.toList());

        List<String> colunasTabela = obterColunasTabela(nomeTabela);

        List<String> cabecalhosValidos = cabecalhosFiltrados.stream()
                .filter(colunasTabela::contains)
                .collect(Collectors.toList());

        if (cabecalhosValidos.isEmpty()) {
            throw new IllegalArgumentException("Nenhuma coluna do CSV corresponde às colunas da tabela");
        }

        inserirDados(linhas.subList(1, linhas.size()), cabecalhos, cabecalhosValidos, nomeTabela, separador);
    }

    private boolean verificarTabelaExiste(String nomeTabela) throws SQLException {
        try (ResultSet rs = connection.getMetaData().getTables(null, null, nomeTabela, null)) {
            return rs.next();
        }
    }

    private List<String> obterColunasTabela(String nomeTabela) throws SQLException {
        List<String> colunas = new ArrayList<>();
        try (ResultSet rs = connection.getMetaData().getColumns(null, null, nomeTabela, null)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                if (!"id".equalsIgnoreCase(columnName)) {
                    colunas.add(columnName);
                }
            }
        }
        return colunas;
    }

    private List<String> lerArquivoCsv(String caminhoCsv) throws IOException {
        Charset[] charsets = {StandardCharsets.UTF_8, StandardCharsets.ISO_8859_1, Charset.forName("Windows-1252")};

        for (Charset charset : charsets) {
            try {
                return Files.readAllLines(Paths.get(caminhoCsv), charset);
            } catch (Exception e) {
                continue;
            }
        }
        throw new IOException("falha na leitura do CSV, charset nn suportado");
    }

    private char detectarSeparador(String primeiraLinha) {
        Character[] separadores = {';', ',', '\t', '|'};
        return Arrays.stream(separadores)
                .max(Comparator.comparingLong(sep ->
                        primeiraLinha.chars().filter(c -> c == sep).count()
                ))
                .orElse(',');
    }

    private String[] parseCSVLinha(String linha, char separador) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < linha.length(); i++) {
            char c = linha.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == separador && !inQuotes) {
                result.add(current.toString().trim());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        result.add(current.toString().trim());
        return result.toArray(new String[0]);
    }

    private String[] parseCSVLinha(String line) {
        return parseCSVLinha(line, ',');
    }

    private Map<String, String> detectarTiposColunas(List<String> linhas, String[] cabecalhos, Set<String> colunasExcluidas, char separador) {
        Map<String, Set<String>> samples = Arrays.stream(cabecalhos)
                .map(String::trim)
                .filter(h -> !h.isEmpty() && !colunasExcluidas.contains(h))
                .collect(Collectors.toMap(h -> h, h -> new HashSet<>()));

        linhas.stream()
                .skip(1)
                .limit(SAMPLE_SIZE)
                .forEach(line -> {
                    String[] values = parseCSVLinha(line, separador);
                    for (int i = 0; i < Math.min(values.length, cabecalhos.length); i++) {
                        String header = cabecalhos[i].trim();
                        if (samples.containsKey(header) && i < values.length && !values[i].trim().isEmpty()) {
                            samples.get(header).add(values[i].trim());
                        }
                    }
                });

        return samples.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> determinarTiposDeDados(entry.getValue())
                ));
    }

    private String determinarTiposDeDados(Set<String> valores) {
        if (valores.isEmpty()) return "TEXT";

        if (valores.stream().allMatch(v -> INTEGER_PATTERN.matcher(v).matches())) {
            long maxValue = valores.stream().mapToLong(Long::parseLong).max().orElse(0);
            return maxValue <= Integer.MAX_VALUE ? "INT" : "BIGINT";
        }

        if (valores.stream().allMatch(v -> DECIMAL_PATTERN.matcher(v).matches() || INTEGER_PATTERN.matcher(v).matches())) {
            return "DECIMAL(15,4)";
        }

        if (valores.stream().allMatch(v -> DATE_PATTERN_BR.matcher(v).matches() || DATE_PATTERN_ISO.matcher(v).matches())) {
            boolean hasTime = valores.stream().anyMatch(v -> v.contains(":"));
            return hasTime ? "DATETIME" : "DATE";
        }

        if (valores.stream().allMatch(v -> BOOLEAN_PATTERN.matcher(v).matches())) {
            return "BOOLEAN";
        }

        int maxLength = valores.stream().mapToInt(String::length).max().orElse(255);
        return maxLength <= 255 ? "VARCHAR(255)" : "TEXT";
    }

    private void criarTabela(String nomeTabela, List<String> cabecalhos, Map<String, String> colunaTipos) throws SQLException {
        for (String cabecalho : cabecalhos) {
            if (!colunaTipos.containsKey(cabecalho)) {
                throw new IllegalStateException("Tipo não definido para a coluna: " + cabecalho);
            }
        }

        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS `").append(nomeTabela).append("` (");
        sql.append("`id` INT AUTO_INCREMENT PRIMARY KEY");

        if (!cabecalhos.isEmpty()) {
            sql.append(", ");
            sql.append(cabecalhos.stream()
                    .map(h -> "`" + h + "` " + colunaTipos.get(h))
                    .collect(Collectors.joining(", ")));
        }
        sql.append(")");

        System.out.println(" SQL gerado: " + sql.toString());

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS `" + nomeTabela + "`");
            stmt.execute(sql.toString());
            System.out.println(" Tabela '" + nomeTabela + "' criada com sucesso!");
        } catch (SQLException e) {
            System.err.println("Erro ao criar tabela: " + e.getMessage());
            System.err.println("SQL: " + sql.toString());
            throw e;
        }
    }

    private void inserirDados(List<String> dadosLinhas, String[] cabecalhosOriginais, List<String> cabecalhosFiltrados, String nomeTabela, char separador) throws SQLException {
        if (cabecalhosFiltrados.isEmpty()) {
            System.out.println("nenhuma coluna para inserir dados");
            return;
        }
        Map<String, Integer> headerIndexMap = IntStream.range(0, cabecalhosOriginais.length)
                .boxed()
                .collect(Collectors.toMap(i -> cabecalhosOriginais[i].trim(), i -> i));

        Map<String, String> tiposColunas = obterTiposColunas(nomeTabela, cabecalhosFiltrados);

        String placeholders = String.join(", ", Collections.nCopies(cabecalhosFiltrados.size(), "?"));
        String columnNames = cabecalhosFiltrados.stream()
                .map(h -> "`" + h + "`")
                .collect(Collectors.joining(", "));

        String sql = "INSERT INTO `" + nomeTabela + "` (" + columnNames + ") VALUES (" + placeholders + ")";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            connection.setAutoCommit(false);
            int batchCount = 0;
            int totalInseridos = 0;

            for (String line : dadosLinhas) {
                String[] values = parseCSVLinha(line, separador);

                for (int i = 0; i < cabecalhosFiltrados.size(); i++) {
                    String header = cabecalhosFiltrados.get(i);
                    Integer index = headerIndexMap.get(header);
                    String value = (index != null && index < values.length) ? values[index].trim() : "";

                    if (value.isEmpty()) {
                        pstmt.setNull(i + 1, Types.NULL);
                    } else {
                        setParameterByType(pstmt, i + 1, value, tiposColunas.get(header));
                    }
                }

                pstmt.addBatch();
                if (++batchCount % BATCH_SIZE == 0) {
                    pstmt.executeBatch();
                    connection.commit();
                    totalInseridos += batchCount;
                    System.out.println("Inserindos " + totalInseridos + " registross...");
                    batchCount = 0;
                }
            }

            if (batchCount > 0) {
                pstmt.executeBatch();
                connection.commit();
                totalInseridos += batchCount;
            }

            connection.setAutoCommit(true);
            System.out.println("Total de " + totalInseridos + " registros inseridos");
        }
    }

    private Map<String, String> obterTiposColunas(String nomeTabela, List<String> colunas) throws SQLException {
        Map<String, String> tipos = new HashMap<>();
        try (ResultSet rs = connection.getMetaData().getColumns(null, null, nomeTabela, null)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String dataType = rs.getString("TYPE_NAME");
                if (colunas.contains(columnName)) {
                    tipos.put(columnName, dataType);
                }
            }
        }
        return tipos;
    }

    private void setParameterByType(PreparedStatement pstmt, int paramIndex, String value, String sqlType) throws SQLException {
        if (value == null || value.isEmpty()) {
            pstmt.setNull(paramIndex, Types.NULL);
            return;
        }

        try {
            switch (sqlType.toUpperCase()) {
                case "DATE":
                    if (DATE_PATTERN_BR.matcher(value).matches()) {
                        String[] parts = value.split("/");
                        String isoDate = parts[2] + "-" + parts[1] + "-" + parts[0];
                        pstmt.setDate(paramIndex, Date.valueOf(isoDate));
                    } else if (DATE_PATTERN_ISO.matcher(value).matches()) {
                        pstmt.setDate(paramIndex, Date.valueOf(value.substring(0, 10)));
                    } else {
                        pstmt.setString(paramIndex, value);
                    }
                    break;
                case "DATETIME":
                case "TIMESTAMP":
                    if (DATE_PATTERN_BR.matcher(value).matches()) {
                        String[] parts = value.split("/");
                        String isoDateTime = parts[2] + "-" + parts[1] + "-" + parts[0] + " 00:00:00";
                        pstmt.setTimestamp(paramIndex, Timestamp.valueOf(isoDateTime));
                    } else if (DATE_PATTERN_ISO.matcher(value).matches()) {
                        if (value.contains(" ")) {
                            pstmt.setTimestamp(paramIndex, Timestamp.valueOf(value));
                        } else {
                            pstmt.setTimestamp(paramIndex, Timestamp.valueOf(value + " 00:00:00"));
                        }
                    } else {
                        pstmt.setString(paramIndex, value);
                    }
                    break;
                case "INT":
                case "INTEGER":
                    pstmt.setInt(paramIndex, Integer.parseInt(value));
                    break;
                case "BIGINT":
                    pstmt.setLong(paramIndex, Long.parseLong(value));
                    break;
                case "DECIMAL":
                case "NUMERIC":
                case "DOUBLE":
                case "FLOAT":
                    pstmt.setBigDecimal(paramIndex, new java.math.BigDecimal(value));
                    break;
                case "BOOLEAN":
                case "BOOL":
                case "TINYINT":
                    if (value.equalsIgnoreCase("true") || value.equals("1") ||
                            value.equalsIgnoreCase("yes") || value.equalsIgnoreCase("y")) {
                        pstmt.setBoolean(paramIndex, true);
                    } else if (value.equalsIgnoreCase("false") || value.equals("0") ||
                            value.equalsIgnoreCase("no") || value.equalsIgnoreCase("n")) {
                        pstmt.setBoolean(paramIndex, false);
                    } else {
                        pstmt.setString(paramIndex, value);
                    }
                    break;
                default:
                    pstmt.setString(paramIndex, value);
                    break;
            }
        } catch (Exception e) {
            System.err.println("⚠️ Erro ao converter valor '" + value + "' para tipo " + sqlType + ": " + e.getMessage());
            pstmt.setString(paramIndex, value);
        }
    }

    @Override
    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                System.out.println("Conexão fechada com sucesso!");
            }
        } catch (SQLException e) {
            System.err.println("❌Falha ao fechar conexão: " + e.getMessage());
        }
    }

    public static void main(String[] argumentos) throws Exception {
        if (argumentos.length < 4) {
            System.out.println("ARGUMENTOS INSUFICIENTES OU INVÁLIDOS. USO:");
            System.out.println();

            System.out.println("Para criar nova tabela: ");
            System.out.println("java -jar CSVImporter-1.0.jar <arquivo_csv.csv> <nome_tabela> <jdbc_url> <usuario> [senha] [colunas_p_ignorar...] [tipos_personalizados...]");
            System.out.println();

            System.out.println("Para adicionar dados: ");
            System.out.println("java -jar CSVImporter-1.0.jar -add <arquivo_csv.csv> <nome_tabela> <jdbc_url> <usuario> [senha]");
            System.out.println();



            System.out.println("PARA IGNROAR A DETECÇÃO AUTOMATICA DE TIPOS E ATRIBUIR TIPOS PERSONALIZADOS use > coluna:TIPO");
            System.out.println();
            System.out.println("EXEMPLO COMPLETO:");
            System.out.println();

            System.out.println("java -jar CSVImporter-1.0.jar dados.csv tabela jdbc:mysql://localhost:3306/db user pass ex tipo codigo:VARCHAR preco:DECIMAL(10,2)");
            System.out.println();

            System.out.println("java -jar CSVImporter-1.0.jar -add <arquivo_csv.csv> <nome_tabela> <jdbc_url> <usuario> [senha]");
            System.out.println();



            System.exit(1);
        }

        boolean modoAdicionar = "-add".equals(argumentos[0]);
        int offset = modoAdicionar ? 1 : 0;

        String arquivoCsv = argumentos[offset];
        String nomeTabela = argumentos[offset + 1];
        String jdbcUrl = argumentos[offset + 2];
        String usuario = argumentos[offset + 3];
        String senha = argumentos.length > offset + 4 ? argumentos[offset + 4] : "";

        try (CSVImporter importer = new CSVImporter(jdbcUrl, usuario, senha)) {
            long start = System.currentTimeMillis();

            if (modoAdicionar) {
                importer.adicionarDadosTabela(arquivoCsv, nomeTabela);
                System.out.println("DADOSS ADICIONANDOS COM SUCESSO!");
            } else {
                Set<String> colunasIgnorar = new HashSet<>();
                Map<String, String> tiposPersonalizados = new HashMap<>();

                if (argumentos.length > offset + 5) {
                    for (int i = offset + 5; i < argumentos.length; i++) {
                        String arg = argumentos[i];
                        if (arg.contains(":")) {
                            String[] partes = arg.split(":", 2);
                            if (partes.length == 2) {
                                String coluna = partes[0].trim();
                                String tipo = partes[1].trim().toUpperCase();
                                tiposPersonalizados.put(coluna, tipo);
                                System.out.println("Tipo personalizado definido: " + coluna + " -> " + tipo);
                            }
                        } else {
                            colunasIgnorar.add(arg);
                            System.out.println("Coluna ignorada: " + arg);
                        }
                    }
                }

                importer.importarCSV(arquivoCsv, nomeTabela, colunasIgnorar, tiposPersonalizados);
                System.out.println("A IMPORTAÇÃO FOI CONCLUÍDA!");
            }

            System.out.println("TEMPO: " + (System.currentTimeMillis() - start) + "ms");
        }catch (Exception e) {
            System.err.println("❌ ERRO: " + e.getMessage());
            e.printStackTrace();
            throw e;
    }}}