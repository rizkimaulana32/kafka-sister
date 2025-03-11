import { Kafka, Partitioners } from "kafkajs";
import readline from "readline";

const kafka = new Kafka({
    brokers: ["localhost:9092"]
});

const producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner
});

await producer.connect();

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

console.log("Format: <topic> <pesan> | Ketik 'exit' untuk keluar");

// Fungsi mengirim pesan ke topic tertentu
const sendMessage = async (topic, message) => {
    await producer.send({
        topic,
        messages: [{ value: message }]
    });
    console.log(`Pesan terkirim ke ${topic}: ${message}`);
};

// Menunggu input dari user
rl.on("line", async (input) => {
    if (input.toLowerCase() === "exit") {
        console.log("Keluar...");
        await producer.disconnect();
        rl.close();
        process.exit(0);
    }

    // Memisahkan topic dan pesan berdasarkan spasi pertama
    const firstSpaceIndex = input.indexOf(" ");
    if (firstSpaceIndex === -1) {
        console.log("Format salah! Gunakan: <topic> <pesan>");
        return;
    }

    const topic = input.substring(0, firstSpaceIndex);
    const message = input.substring(firstSpaceIndex + 1);

    if (!topic || !message) {
        console.log("Topic dan pesan tidak boleh kosong!");
        return;
    }

    await sendMessage(topic, message);
});
