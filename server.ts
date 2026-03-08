import express, { Request, Response } from 'express';
import cors from 'cors';
import * as dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';

// 1. Inicializar variables de entorno
dotenv.config();

// 2. Crear la aplicación de Express (Tu servidor web)
const app = express();
const port = process.env.PORT || 3000;

// 3. Middlewares (Filtros de seguridad y formato)
app.use(cors()); // Permite que tu futura página web (Frontend) haga peticiones sin ser bloqueada
app.use(express.json()); // Permite que el servidor entienda paquetes de datos en formato JSON

// 4. Inicializar el SDK de Supabase (Tu ORM)
const supabaseUrl = process.env.SUPABASE_URL!;
const supabaseKey = process.env.SUPABASE_ANON_KEY!;
const supabase = createClient(supabaseUrl, supabaseKey);

// 5. TU PRIMER ENDPOINT: Ruta GET para obtener el clima actual
app.get('/api/clima/actual', async (req: Request, res: Response) => {
    try {
        // Esto es el equivalente directo a usar LINQ o Entity Framework en C#
        const { data, error } = await supabase
            .from('lecturas_meteorologicas')
            .select('*')
            .order('timestamp_lectura', { ascending: false }) // Ordenamos por fecha descendente
            .limit(1); // Tomamos solo el primer registro (el más nuevo)

        // Si la base de datos nos arroja un error, lo lanzamos al Catch
        if (error) throw error;

        // Si no hay datos aún, devolvems un 404 (Not Found)
        if (!data || data.length === 0) {
            return res.status(404).json({ mensaje: 'No hay datos meteorológicos disponibles.' });
        }

        // Respondemos con código 200 (OK) y enviamos el primer objeto del arreglo
        res.status(200).json(data[0]);

    } catch (error: any) {
        console.error("❌ Error al consultar Supabase:", error.message);
        // Respondemos con un código 500 (Internal Server Error) para no tumbar el servidor
        res.status(500).json({ error: 'Error interno del servidor al obtener el clima.' });
    }
});

// 6. Encender el motor del servidor
app.listen(port, () => {
    console.log(`\n🌐 Servidor API corriendo exitosamente en el puerto ${port}`);
    console.log(`📡 Endpoint disponible en: http://localhost:${port}/api/clima/actual`);
});