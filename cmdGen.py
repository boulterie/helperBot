from generator import LicenseGenerator
import argparse

def main():
    parser = argparse.ArgumentParser(description="cmdGen")
    parser.add_argument("-c", "--customer", help="Имя пользователя", required=True)  # Обязательный аргумент
    parser.add_argument("-t", "--time", help="Срок действия (week/month/year/unlimited)", required=True)

    args = parser.parse_args()

    generator = LicenseGenerator()
    generator.load_licenses()

    license = generator.create_license(
        customer=args.customer,
        time=args.time
    )

    generator.save_licenses()
    print(f"Сгенерированный ключ: {license['key']}\nПокупатель: {license['customer']}\nСрок действия: {license['period']} дней\nВремя генерации: {license['created']} (МСК)")



if __name__ == "__main__":
    main()